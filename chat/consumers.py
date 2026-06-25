import json
from channels.generic.websocket import AsyncWebsocketConsumer


class ChatConsumer(AsyncWebsocketConsumer):
    connected_count = 0

    async def connect(self):
        self.room_name = "test_room"
        self.room_group_name = f"chat_{self.room_name}"

        await self.channel_layer.group_add(        #group_add(group_name, channel_name) <- ある channel（接続）を、指定した group に所属させる処理
            self.room_group_name,                  #第一引数 = グループ名, 第二引数 = 個々の接続を識別する一意ID
            self.channel_name,
        )

        ChatConsumer.connected_count += 1

        await self.accept()

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": ChatConsumer.connected_count,
            },
        )

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name,
        )

        ChatConsumer.connected_count -= 1

        await self.channel_layer.group_send(
            self.room_group_name,
            {
                "type": "viewer_update",
                "count": ChatConsumer.connected_count,
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