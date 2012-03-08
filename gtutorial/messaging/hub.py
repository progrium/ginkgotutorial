import gevent.queue
from ginkgo.core import Service, autospawn

from .http import HttpStreamer
from .http import HttpTailViewer
from .websocket import WebSocketStreamer
from .backend import MessageBackend

class MessageHub(Service):
    def __init__(self, cluster=None, bind_interface=None):
        self.bind_interface = bind_interface or '0.0.0.0'

        self.backend = MessageBackend(cluster, bind_interface)
        self.add_service(self.backend)

        self.add_service(HttpStreamer(self))
        self.add_service(HttpTailViewer(self))
        self.add_service(WebSocketStreamer(self))

    def publish(self, channel, message):
        self.backend.publish(channel, message)

    def subscribe(self, channel):
        return self.backend.subscribe(channel)

