
class Leadership(Service):
    cluster = Setting('cluster', default=[])
    port = Setting('leader_port', default=12345)
    heartbeat_interval = Setting('leader_heartbeat_interval_secs', default=5)

    def __init__(self, identity, context):
        self._identity = identity
        self._candidates = sorted(self.cluster)
        self._promoted = Event()
        self._broadcaster = context.socket(zmq.PUB)
        self._listener = context.socket(zmq.SUB)
        self._listener.setsockopt(zmq.SUBSCRIBE, '')

    @property
    def is_leader(self):
        return self._identity is self._leader

    def wait_for_promotion(self):
        self._promoted.wait()

    def do_start(self):
        self._broadcaster.bind("tcp://0.0.0.0:{}".format(self.port))
        self._broadcast_when_promoted()
        self._listen_for_heartbeats()
        self._next_leader()

    def _next_leader(self):
        self._leader = self._candidates.pop(0)
        if self.is_leader:
            self._promoted.set()
        else:
            self._listener.connect("tcp://{}:{}".format(self._leader, self.port))

    @autospawn
    def _broadcast_when_promoted(self):
        self.wait_for_promotion()
        while self.is_leader:
            self._broadcaster.send(self._identity)
            gevent.sleep(self.heartbeat_interval)

    @autospawn
    def _listen_for_heartbeats(self):
        while not self.is_leader:
            with Timeout(self.heartbeat_interval * 2, False) as timeout:
                leader = self._listener.recv()
            if leader is None:
                self._next_leader()


class Announcer(Service):
    def __init__(self, hub, cluster, identity):
        self.hub = hub
        self.cluster = cluster
        self.identity = identity

    def do_start(self):
        self._announce()

    @autospawn
    def _announce(self):
        while True:
            cluster_snapshot = ordered(list(self.cluster))
            identity_index = cluster_snapshot.index(self.identity)
            announcer_index = int(time.time()) % len(cluster_snapshot)
            if announcer_index is indetity_index:
                self.hub.publish("/announce", self.identity)
            gevent.sleep(1)

