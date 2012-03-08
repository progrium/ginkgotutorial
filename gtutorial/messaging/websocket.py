import gevent
import logging

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

from ws4py.server.geventserver import WebSocketServer

logger = logging.getLogger(__name__)

class WebSocketStreamer(Service):
    address = Setting('websocket_bind', default=('0.0.0.0', 7070))

    def __init__(self, hub):
        self.hub = hub

        self.add_service(
            WebSocketServer(self.address, self.handle))

    def handle(self, websocket, environ):
        channel = environ.get('PATH_INFO')
        subscription = self.hub.subscribe(channel)
        for msg in subscription:
            try:
                websocket.send(msg)
                gevent.sleep(0)
            except IOError:
                break

