import json
from channels.generic.websocket import AsyncWebsocketConsumer


class ChatConsumer(AsyncWebsocketConsumer):
    connected_counts = {}

    async def connect(self):
        self.room_name = self.scope["url_route"]["kwargs"]["room_name"]
        self.room_group_name = f"chat_{self.room_name}"

        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name,
        )

        ChatConsumer.connected_counts[self.room_name] = (
            ChatConsumer.connected_counts.get(self.room_name, 0) + 1
        )

        await self.accept()

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": ChatConsumer.connected_counts[self.room_name],
            },
        )

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name,
        )

        ChatConsumer.connected_counts[self.room_name] -= 1

        if ChatConsumer.connected_counts[self.room_name] <= 0:
            del ChatConsumer.connected_counts[self.room_name]

        count = ChatConsumer.connected_counts.get(self.room_name, 0)

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": count,
            },
        )

    async def receive(self, text_data):
        data = json.loads(text_data)
        message = data["message"]

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "chat_message",
                "message": message,
            },
        )

    async def chat_message(self, event):
        await self.send(
            text_data=json.dumps(
                {
                    "message": event["message"],
                }
            )
        )

    async def viewer_update(self, event):
        await self.send(
            text_data=json.dumps(
                {
                    "viewer_count": event["count"],
                }
            )
        )