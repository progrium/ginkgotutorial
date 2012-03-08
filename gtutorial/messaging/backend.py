import gevent.queue
import msgpack

from gevent_zeromq import zmq

from ginkgo.core import Service, require_ready, autospawn
from ginkgo.config import Setting

from ..util import ObservableSet

class Subscription(gevent.queue.Queue):
    def __init__(self, receiver, channel):
        super(Subscription, self).__init__(maxsize=64)
        self.channel = channel
        self.receiver = receiver
        self.receiver.subscribe(channel, self)

    def cancel(self):
        self.receiver.unsubscribe(self.channel, self)
        self.channel = None

class MessageBackend(Service):
    port = Setting('backend_port', default=2222)

    def __init__(self, cluster=None, bind_interface=None, zmq_=None):
        self.cluster = cluster or ObservableSet()
        self.zmq = zmq_ or zmq.Context()

        self.transmitter = PeerTransmitter(self)
        self.receiver = PeerReceiver(self, bind_interface)

        self.add_service(self.transmitter)
        self.add_service(self.receiver)

    def publish(self, channel, message):
        self.transmitter.broadcast(channel, message)

    def subscribe(self, channel):
        return Subscription(self.receiver, channel)

class PeerTransmitter(Service):
    def __init__(self, backend):
        self.cluster = backend.cluster
        self.port = backend.port
        self.socket = backend.zmq.socket(zmq.PUB)

    def do_start(self):
        def connector(add=None, remove=None):
            if add: self.connect(add)
        self.cluster.attach(connector)
        for host in self.cluster:
            self.connect(host)

    def connect(self, host):
        self.socket.connect("tcp://{}:{}".format(host, self.port))

    @require_ready
    def broadcast(self, channel, message):
        self.socket.send_multipart([str(channel).lower(), msgpack.packb(message)])

class PeerReceiver(Service):
    def __init__(self, backend, bind_interface=None):
        self.bind_address = (bind_interface or '0.0.0.0', backend.port)
        self.socket = backend.zmq.socket(zmq.SUB)
        self.subscriptions = dict()

    def do_start(self):
        self.socket.bind("tcp://{}:{}".format(*self.bind_address))
        self._listen()

    def subscribe(self, channel, subscriber):
        channel = str(channel).lower()

        if self.subscriptions.get(channel) is None:
            self.subscriptions[channel] = set()

        self.socket.setsockopt(zmq.SUBSCRIBE, channel)
        self.subscriptions[channel].add(subscriber)

    def unsubscribe(self, channel, subscriber):
        channel = str(channel).lower()

        self.socket.setsockopt(zmq.UNSUBSCRIBE, channel)
        self.subscriptions[channel].remove(subscriber)

    @autospawn
    def _listen(self):
        while True:
            channel, message = self.socket.recv_multipart()
            for subscription in self.subscriptions.get(channel, []):
                subscription.put(msgpack.unpackb(message))
