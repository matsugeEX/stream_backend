import json
import redis.asyncio as redis
from channels.generic.websocket import AsyncWebsocketConsumer


redis_client = redis.Redis(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True,
)


class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.room_name = self.scope["url_route"]["kwargs"]["room_name"]
        self.room_group_name = f"chat_{self.room_name}"
        self.viewer_key = f"viewers:{self.room_name}"

        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name,
        )

        await self.accept()

        count = await redis_client.incr(self.viewer_key)

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": count,
            },
        )

    async def disconnect(self, close_code):
        count = await redis_client.decr(self.viewer_key)

        if count < 0:
            count = 0
            await redis_client.set(self.viewer_key, 0)

        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name,
        )

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": count,
            },
        )

    async def receive(self, text_data):
        data = json.loads(text_data)
        message_type = data.get("type")

        if message_type == "chat":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "chat_message",
                    "message": data["message"],
                },
            )

        elif message_type == "webrtc_offer":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_offer",
                    "sdp": data["sdp"],
                    "sender_channel_name": self.channel_name,
                },
            )

        elif message_type == "webrtc_answer":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_answer",
                    "sdp": data["sdp"],
                    "sender_channel_name": self.channel_name,
                },
            )

        elif message_type == "webrtc_candidate":
            await self.channel_layer.group_send(
                self.room_group_name,
                {
                    "type": "webrtc_candidate",
                    "candidate": data["candidate"],
                    "sender_channel_name": self.channel_name,
                },
            )

    async def chat_message(self, event):
        await self.send(text_data=json.dumps({
            "type": "chat",
            "message": event["message"],
        }))

    async def viewer_update(self, event):
        await self.send(text_data=json.dumps({
            "type": "viewer_count",
            "count": event["count"],
        }))
    
    async def webrtc_offer(self, event):
        if event["sender_channel_name"] == self.channel_name:
            return

        await self.send(
            text_data=json.dumps({
                "type": "webrtc_offer",
                "sdp": event["sdp"],
            })
        )


    async def webrtc_answer(self, event):
        if event["sender_channel_name"] == self.channel_name:
            return
        await self.send(text_data=json.dumps({
            "type": "webrtc_answer",
            "sdp": event["sdp"],
        }))


    async def webrtc_candidate(self, event):
        if event["sender_channel_name"] == self.channel_name:
            return
        await self.send(text_data=json.dumps({
            "type": "webrtc_candidate",
            "candidate": event["candidate"],
        }))