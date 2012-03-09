import gevent.queue
from ginkgo.core import Service, autospawn

from .http import HttpStreamer
from .http import HttpTailViewer
from .websocket import WebSocketStreamer

class Subscription(gevent.queue.Queue):
    def __init__(self, channel):
        super(Subscription, self).__init__(maxsize=64)
        self.channel = channel

    def cancel(self):
        self.channel = None

class MessageHub(Service):
    def __init__(self):
        self.subscriptions = {}

        self.add_service(HttpStreamer(self))
        self.add_service(HttpTailViewer(self))
        self.add_service(WebSocketStreamer(self))

    def publish(self, channel, message):
        for subscription in self.subscriptions.get(channel, []):
            subscription.put(message)

    def subscribe(self, channel):
        if channel not in self.subscriptions:
            self.subscriptions[channel] = []
        subscription = Subscription(channel)
        self.subscriptions[channel].append(subscription)
        return subscription

