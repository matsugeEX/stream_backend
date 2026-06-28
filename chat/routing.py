from django.urls import re_path
from .consumers import StreamConsumer

websocket_urlpatterns = [
    re_path(r"ws/stream/(?P<room_name>\w+)/$", StreamConsumer.as_asgi()),
]