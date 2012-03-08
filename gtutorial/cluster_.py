import logging
import time

import gevent
from gevent import Timeout
from gevent.event import Event
from gevent_zeromq import zmq

from gservice.core import Service, autospawn
from gservice.config import Setting

from .util import ObservableSet

logger = logging.getLogger(__name__)

class ClusterCoordinator(Service):
    updates_port = Setting('cluster_updates_port', default=4440)
    heartbeat_port = Setting('cluster_heartbeat_port', default=4441)
    greeter_port = Setting('cluster_greeter_port', default=4442)
    heartbeat_interval = Setting('cluster_heartbeat_interval_secs', default=2)

    def __init__(self, identity, leader=None, cluster=None, zmq_=None):
        self._zmq = zmq_ or zmq.Context()
        self._cluster = cluster or ObservableSet()
        self._leader = leader or identity
        self._identity = identity
        self._promoted = Event()

        self._server = PeerServer(self)
        self._client = PeerClient(self)

        self._greeter = self._zmq.socket(zmq.REP)

        self.add_service(self._server)

        if self.is_leader:
            self._promoted.set()
        else:
            self.add_service(self._client)

    @property
    def is_leader(self):
        return self._identity == self._leader

    def wait_for_promotion(self):
        self._promoted.wait()

    def do_start(self):
        self._cluster.add(self._identity)
        self._greeter.bind("tcp://{}:{}".format(self._identity, self.greeter_port))
        self._greet()

    @autospawn
    def _greet(self):
        while True:
            self._greeter.recv() # HELLO
            if self.is_leader:
                self._greeter.send_multipart(['WELCOME', ''])
            else:
                response = self.scout(self._leader, 1)
                if len(response):
                    logger.debug("A follower is redirected to {}.".format(self._leader))
                    self._greeter.send_multipart(['REDIRECT', self._leader])
                else:
                    logger.debug("A follower triggers new leader election.")
                    self._client._next_leader()
                    self._greeter.send_multipart(['RETRY', ''])

    def scout(self, leader, timeout=2):
        scout = self._zmq.socket(zmq.REQ)
        scout.connect("tcp://{}:{}".format(leader, self.greeter_port))
        scout.send('HELLO')
        response = []
        with Timeout(timeout, False):
            response = scout.recv_multipart()
        scout.close()
        return response

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
        self._poll_leader()

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
                logger.debug("Leader {} confirmed with warm welcome.".format(leader))
                return leader
            elif reply == 'REDIRECT':
                logger.debug("Leader {} is not actual leader, redirecting...".format(leader))
                return self._confirm_leader(redirect_address)
            elif reply == 'RETRY':
                logger.debug("Leader {} is confused, try again...".format(leader))
                return self._confirm_leader(leader)
        raise Exception("Unable to confirm leader")

    def _next_leader(self):
        self._following.clear()
        self.c._cluster.remove(self.c._leader)
        candidates = sorted(list(self.c._cluster))
        self.c._leader = candidates[0]
        logger.debug("A new leader is decided: {}".format(candidates[0]))
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
            cluster = self._listener.recv_multipart()
            logger.debug("Got cluster update")
            self.c._cluster.replace(set(cluster))

    @autospawn
    def _poll_leader(self):
        while True:
            gevent.sleep(self.c.heartbeat_interval)
            response = self.c.scout(self.c._leader)
            if not response:
                self._next_leader()


class PeerServer(Service):
    def __init__(self, coordinator):
        self.c = coordinator
        self._updates = self.c._zmq.socket(zmq.PUB)
        self._heartbeats = self.c._zmq.socket(zmq.PULL)
        self._latest_heartbeats = {}

        def updater(add=None, remove=None):
            if add: self._broadcast_cluster()
        self.c._cluster.attach(updater)


    def do_start(self):
        self._updates.bind("tcp://{}:{}".format(self.c._identity, self.c.updates_port))
        self._heartbeats.bind("tcp://{}:{}".format(self.c._identity, self.c.heartbeat_port))
        self._send_heartbeats()
        self._receive_heartbeats()
        self._timeout_peers()

    def _broadcast_cluster(self):
        self._updates.send_multipart([self.c._identity] + list(self.c._cluster))

    @autospawn
    def _send_heartbeats(self):
        while True:
            self.c.wait_for_promotion()
            logger.debug("Broadcasting cluster.")
            self._broadcast_cluster()
            gevent.sleep(self.c.heartbeat_interval)

    @autospawn
    def _receive_heartbeats(self):
        while True:
            self.c.wait_for_promotion()
            follower = self._heartbeats.recv()
            self.c._cluster.add(follower) # ignored if already added
            self._latest_heartbeats[follower] = time.time()

    @autospawn
    def _timeout_peers(self):
        while True:
            to_remove = []
            for follower in self._latest_heartbeats:
                time_since_last = time.time() - self._latest_heartbeats[follower]
                if time_since_last > self.c.heartbeat_interval * 2:
                    to_remove.append(follower)
            for follower in to_remove:
                self.c._cluster.remove(follower)
                del self._latest_heartbeats[follower]
            gevent.sleep(self.c.heartbeat_interval)


