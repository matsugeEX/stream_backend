import json
import uuid
from channels.generic.websocket import AsyncWebsocketConsumer
import redis.asyncio as redis


class StreamConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_name = self.scope["url_route"]["kwargs"]["room_name"]
        self.room_group_name = f"stream_{self.room_name}"

        self.redis = redis.from_url("redis://redis:6379/0")
        self.viewer_count_key = f"stream:{self.room_name}:viewer_count"

        self.user_type = None
        self.viewer_id = None

        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name,
        )

        await self.accept()

    async def disconnect(self, close_code):
        if self.user_type == "viewer" and self.viewer_id:
            count = await self.redis.decr(self.viewer_count_key)

            if count < 0:
                count = 0
                await self.redis.set(self.viewer_count_key, 0)

            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "viewer_count",
                    "count": count,
                },
            )

            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "viewer_left",
                    "viewer_id": self.viewer_id,
                },
            )

        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name,
        )

        await self.redis.aclose()

    async def receive(self, text_data):
        data = json.loads(text_data)
        message_type = data.get("type")

        if message_type == "join_streamer":
            self.user_type = "streamer"

            await self.send(text_data=json.dumps({
                "type": "joined_as_streamer"
            }))

        elif message_type == "join_viewer":
            self.user_type = "viewer"
            self.viewer_id = str(uuid.uuid4())

            await self.send(text_data=json.dumps({
                "type": "joined_as_viewer",
                "viewer_id": self.viewer_id,
            }))

            count = await self.redis.incr(self.viewer_count_key)

            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "viewer_count",
                    "count": count,
                },
            )

            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "viewer_joined",
                    "viewer_id": self.viewer_id,
                },
            )

        elif message_type == "webrtc_offer":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_offer",
                    "viewer_id": data["viewer_id"],
                    "sdp": data["sdp"],
                },
            )

        elif message_type == "webrtc_answer":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_answer",
                    "viewer_id": data["viewer_id"],
                    "sdp": data["sdp"],
                },
            )

        elif message_type == "webrtc_candidate":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_candidate",
                    "viewer_id": data["viewer_id"],
                    "candidate": data["candidate"],
                    "sender": self.user_type,
                },
            )

    async def viewer_joined(self, event):
        if self.user_type == "streamer":
            await self.send(text_data=json.dumps({
                "type": "viewer_joined",
                "viewer_id": event["viewer_id"],
            }))
    
    async def viewer_count(self, event):
        await self.send(text_data=json.dumps({
            "type": "viewer_count",
            "count": event["count"],
        }))

    async def viewer_left(self, event):
        if self.user_type == "streamer":
            await self.send(text_data=json.dumps({
                "type": "viewer_left",
                "viewer_id": event["viewer_id"],
            }))

    async def webrtc_offer(self, event):
        if self.user_type == "viewer" and self.viewer_id == event["viewer_id"]:
            await self.send(text_data=json.dumps({
                "type": "webrtc_offer",
                "viewer_id": event["viewer_id"],
                "sdp": event["sdp"],
            }))

    async def webrtc_answer(self, event):
        if self.user_type == "streamer":
            await self.send(text_data=json.dumps({
                "type": "webrtc_answer",
                "viewer_id": event["viewer_id"],
                "sdp": event["sdp"],
            }))

    async def webrtc_candidate(self, event):
        if event["sender"] == "streamer":
            if self.user_type == "viewer" and self.viewer_id == event["viewer_id"]:
                await self.send(text_data=json.dumps({
                    "type": "webrtc_candidate",
                    "viewer_id": event["viewer_id"],
                    "candidate": event["candidate"],
                }))

        elif event["sender"] == "viewer":
            if self.user_type == "streamer":
                await self.send(text_data=json.dumps({
                    "type": "webrtc_candidate",
                    "viewer_id": event["viewer_id"],
                    "candidate": event["candidate"],
                }))