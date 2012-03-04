import logging

import gevent

from gservice.core import Service, autospawn
from gservice.config import Setting

from .client.numbers import NumberClient
from .server.pubsub import MessageHub
from .server.websocket import WebSocketInterface

logger = logging.getLogger(__name__)

class NumberWebBridge(Service):
    def __init__(self):
        self.hub = MessageHub()
        self.client = NumberClient(('127.0.0.1', 7776))
        self.ws = WebSocketInterface(self.hub)

        self.add_service(self.hub)
        self.add_service(self.client)
        self.add_service(self.ws)

    def do_start(self):
        self._bridge()

    @autospawn
    def _bridge(self):
        for number in self.client:
            self.hub.publish('/numbers', number)
            gevent.sleep(0)
