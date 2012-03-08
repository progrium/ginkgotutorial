import time
import gevent
from gevent import Timeout
from gevent.event import Event
from gevent_zeromq import zmq

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

class Leadership(Service):
    port = Setting('leader_port', default=12345)
    heartbeat_interval = Setting('leader_heartbeat_interval_secs', default=3)

    def __init__(self, identity, cluster, zmq_=None):
        zmq_ = zmq_ or zmq.Context()
        self.identity = identity
        self.leader = None
        self.set = cluster
        self._candidates = sorted(list(cluster))
        self._promoted = Event()
        self._broadcaster = zmq_.socket(zmq.PUB)
        self._listener = zmq_.socket(zmq.SUB)
        self._listener.setsockopt(zmq.SUBSCRIBE, '')

    @property
    def is_leader(self):
        return self.identity == self.leader

    def wait_for_promotion(self):
        self._promoted.wait()

    def do_start(self):
        self._broadcaster.bind("tcp://{}:{}".format(self.identity, self.port))
        self._broadcast_when_promoted()
        self._listen_for_heartbeats()
        self._next_leader()

    def _next_leader(self):
        self.leader = self._candidates.pop(0)
        if self.is_leader:
            self._promoted.set()
        else:
            self._listener.connect("tcp://{}:{}".format(self.leader, self.port))

    @autospawn
    def _broadcast_when_promoted(self):
        self.wait_for_promotion()
        while self.is_leader:
            self._broadcaster.send(self.identity)
            gevent.sleep(self.heartbeat_interval)

    @autospawn
    def _listen_for_heartbeats(self):
        while not self.is_leader:
            leader = None
            with Timeout(self.heartbeat_interval * 2, False) as timeout:
                leader = self._listener.recv()
            if leader is None:
                self._next_leader()


