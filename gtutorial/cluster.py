import time

import gevent
from gevent import Timeout
from gevent.event import Event
from gevent_zeromq import zmq

from gservice.core import Service
from gservice.config import Setting


class ClusterCoordinator(Service):
    updates_port = Setting('cluster_updates_port', default=4440)
    heartbeat_port = Setting('cluster_heartbeat_port', default=4441)
    greeter_port = Setting('cluster_greeter_port', default=4442)
    heartbeat_interval = Setting('cluster_heartbeat_interval_secs', default=5)

    def __init__(self, identity, leader=None, cluster=None, zmq_=None):
        self._zmq = zmq_ or zmq.Context()
        self._leader = leader or identity
        self._identity = identity
        self._cluster = cluster
        self._promoted = Event()

        self._server = PeerServer(self)
        self._client = PeerClient(self)

        self._greeter = self._zmq.socket(zmq.REP)

        self.add_service(self._server)
        self.add_service(self._client)

    @property
    def is_leader(self):
        return self._identity is self._leader

    def wait_for_promotion(self):
        self._promoted.wait()

    def do_start(self):
        self._greeter.bind("tcp://0.0.0.0:{}".format(self.greeter_port))
        self._greet()

    @autospawn
    def _greet(self):
        while True:
            self._greeter.recv() # HELLO
            if self.is_leader:
                self._greeter.send_multipart(['WELCOME', ''])
            else:
                response = self.scout(self._leader)
                if len(response):
                    self._greeter.send_multipart(['REDIRECT', self._leader])
                else:
                    self._client._next_leader()
                    self._greeter.send_multipart(['RETRY', ''])

    def scout(self, leader):
        scout = self._zmq.socket(zmq.REQ)
        scout.connect("tcp://{}:{}".format(leader, self.greeter_port))
        scout.send('HELLO')
        with Timeout(2,	False):
            response = scout.recv_multipart()
        scount.close()
        return reply

class PeerClient(Service):
    def __init__(self, coordinator):
        self.c = coordinator
        self._following = Event()
        self._listener = None
        self._heartbeater = None

    def do_start(self):
        self._follow_leader()
        self._send_heartbeats()
        self._listen_for_updates()

    def _follow_leader(self):
        self.c._leader = self._confirm_leader(self.c._leader)

        if self._listener is not None:
            self._listener.close()
        self._listener = self.c._zmq.socket(zmq.SUB)
        self._listener.setsockopt(zmq.SUBSCRIBE, '')
        self._listener.connect("tcp://{}:{}".format(self.c._leader,
            self.c.updates_port))

        if self._heartbeater is not None:
            self._heartbeater.close()
        self._heartbeater = self.c._zmq.socket(zmq.PUSH)
        self._heartbeater.connect("tcp://{}:{}".format(self.c._leader,
            self.c.heartbeat_port))

        self._following.set()

    def _confirm_leader(self, leader):
        response = self.c.scout(leader)
        if len(response):
            reply, redirect_address = response
            if reply == 'WELCOME':
                return leader
            elif reply == 'REDIRECT':
                return self._confirm_leader(redirect_address)
            elif reply == 'RETRY':
                return self._confirm_leader(leader)
        raise Exception("Unable to confirm leader")

    def _next_leader(self):
        self._following.clear()
        self.c._cluster.remove(self.c._leader)
        candidates = ordered(list(self.c._cluster))
        self.c._leader = candidates[0]
        if self.c.is_leader:
            self.c._promoted.set()
        else:
            self._follow_leader()

    @autospawn
    def _send_heartbeats(self):
        while True:
            self._following.wait()
            self._heartbeater.send(self.c._identity)
            gevent.sleep(self.c.heartbeat_interval)

    @autospawn
    def _listen_for_updates(self):
        while True:
            self._following.wait()
            with Timeout(self.c.heartbeat_interval * 2, False) as timeout:
                cluster = self._listener.recv_multipart()
            if cluster is None:
                # Leader heartbeat timeout, promote new leader
                self._next_leader()
            else:
                self._cluster.replace(set(cluster))

class PeerServer(Service):
    def __init__(self, coordinator):
        self.c = coordinator
        self._updates = self.c._zmq.socket(zmq.PUB)
        self._heartbeats = self.c._zmq.socket(zmq.PULL)

        def updater(add=None, remove=None):
            if add: self._broadcast_cluster()
        self.c._cluster.attach(updater)


    def do_start(self):
        self._updates.bind("tcp://0.0.0.0:{}".format(self.c.updates_port))
        self._heartbeats.bind("tcp://0.0.0.0:{}".format(self.c.heartbeat_port))
        self._send_heartbeats()
        self._receive_heartbeats()

    def _broadcast_cluster(self):
        self._updates.send_multipart(list(self.c._cluster))

    @autospawn
    def _send_heartbeats(self):
        while True:
            self.c.wait_for_promotion()
            self._broadcast_cluster()
            gevent.sleep(self.c.heartbeat_interval)

    @autospawn
    def _receive_heartbeats(self):
        last_heartbeats = {}
        while True:
            self.c.wait_for_promotion()
            follower = self._heartbeats.recv()
            if follower in last_heartbeats:
                time_since_last = time.time() - last_heartbeats[follower]
                if time_since_last > self.c.heartbeat_interval * 2:
                    self.c._cluster.remove(follower)
                    del last_heartbeats[follower]
                    continue
            self.c._cluster.add(follower) # ignored if already added
            last_heartbeat[follower] = time.time()

