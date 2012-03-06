import gevent.queue
from gservice.core import Service, autospawn

from .http import HttpStreamer
from .http import HttpTailViewer

class Subscription(gevent.queue.Queue):
    def __init__(self, channel):
        super(Subscription, self).__init__(maxsize=64)
        self.channel = channel
        self.channel.append(self)

    def cancel(self):
        self.channel.remove(self)
        self.channel = None

class MessageHub(Service):
    def __init__(self):
        self.add_service(HttpStreamer(self))
        self.add_service(HttpTailViewer(self))
        self.channels = {} # dict of lists of subscriptions

    @autospawn
    def publish(self, channel, message):
        if channel in self.channels:
            for subscriber in self.channels[channel]:
                subscriber.put(message)

    def subscribe(self, channel):
        if channel not in self.channels:
            self.channels[channel] = []
        return Subscription(self.channels[channel])

