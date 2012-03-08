import logging
import time
import json

import gevent
from gevent import Timeout
import gevent.server
from gevent.event import Event
from gevent_zeromq import zmq

from ginkgo.core import Service, autospawn, NOT_READY
from ginkgo.config import Setting
from ginkgo import util

from .util import ObservableSet

CLIENT_TIMEOUT_SECONDS = 10
SERVER_KEEPALIVE_SECONDS = 5

logger = logging.getLogger(__name__)

class ClusterError(Exception): pass
class NewLeader(Exception): pass

class ClusterCoordinator(Service):
    port = Setting('cluster_port', default=4440)

    def __init__(self, identity, leader=None, cluster=None):
        leader = leader or identity
        self.server = PeerServer(self, identity)
        self.client = PeerClient(self, leader, identity)
        self.set = cluster or ObservableSet()
        self.promoted = Event()

        self.add_service(self.server)
        if leader != identity:
            self.add_service(self.client)
            self.is_leader = False
        else:
            self.is_leader = True

    def wait_for_promotion(self):
        self.promoted.wait()

    @property
    def leader(self):
        return self.client.leader

    @property
    def identity(self):
        return self.client.identity

class PeerServer(Service):
    def __init__(self, coordinator, identity):
        self.c = coordinator
        self.identity = identity
        self.clients = {}
        self.server = gevent.server.StreamServer((identity, self.c.port), 
                        handle=self.handle, spawn=self.spawn)

        self.add_service(self.server)

    def do_start(self):
        if self.c.is_leader:
            self.c.set.add(self.identity)

    def handle(self, socket, address):
        """
        If not a leader, a node will simply return a single item list pointing
        to the leader. Otherwise, it will add the host of the connected client
        to the cluster roster, broadcast to all nodes the new roster, and wait
        for keepalives. If no keepalive within timeout or the client drops, it
        drops it from the roster and broadcasts to all remaining nodes. 
        """
        if not self.c.is_leader:
            socket.send(json.dumps({'leader': self.c.client.leader, 
                'port': self.c.port}))
            socket.close()
            logger.debug("Redirected to %s:%s" % (self.c.client.leader, self.c.port))
        else:
            socket.send(self._cluster_message())
            sockfile = socket.makefile()
            name = sockfile.readline()
            if not name:
                return
            if name == '\n':
                name = address[0]
            else:
                name = name.strip()
            logger.debug('New connection from %s' % name)
            self._update(add={'host': name, 'socket': socket})
            # TODO: Use TCP keepalives
            timeout = self._client_timeout(socket)
            for line in util.line_protocol(sockfile, strip=False):
                timeout.kill()
                timeout = self._client_timeout(socket)
                socket.send('\n')
                #logger.debug("Keepalive from %s:%s" % address)
            #logger.debug("Client disconnected from %s:%s" % address)
            self._update(remove=name)

    def _client_timeout(self, socket):
        def shutdown(socket):
            try:
                socket.shutdown(0)
            except IOError:
                pass
        return self.spawn_later(CLIENT_TIMEOUT_SECONDS, 
                lambda: shutdown(socket))

    def _cluster_message(self):
        return '%s\n' % json.dumps({'cluster': list(self.c.set)})

    def _update(self, add=None, remove=None):
        """ Used by leader to manage and broadcast roster """
        if add is not None:
            self.c.set.add(add['host'])
            self.clients[add['host']] = add['socket']
            #logger.debug("Added to cluster: %s" % add['host'])
        if remove is not None:
            self.c.set.remove(remove)
            del self.clients[remove]
            #logger.debug("Removed from cluster: %s" % remove)
        for client in self.clients:
            self.clients[client].send(self._cluster_message())


class PeerClient(Service):
    def __init__(self, coordinator, leader, identity):
        self.c = coordinator
        self.leader = leader
        self.identity = identity

    def do_start(self):
        self.spawn(self.connect)
        return NOT_READY

    def connect(self):
        while self.leader != self.identity:
            address = (self.leader, self.c.port)
            logger.debug("Connecting to leader at %s:%s" % address)
            try:
                socket = util.connect_and_retry(address, max_retries=5)
            except IOError:
                raise ClusterError("Unable to connect to leader %s:%s" % 
                                                    address)
            self.handle(socket)

    def handle(self, socket):
        self.set_ready()
        #logger.debug("Connected to leader")
        client_address = self.identity or socket.getsockname()[0]
        socket.send('%s\n' % client_address)
        # TODO: Use TCP keepalives
        keepalive = self._server_keepalive(socket)
        try:
            for line in util.line_protocol(socket, strip=False):
                if line == '\n':
                    # Keepalive ack from leader
                    keepalive.kill()
                    keepalive = self._server_keepalive(socket)
                else:
                    cluster = json.loads(line)
                    if 'leader' in cluster:
                        # Means you have the wrong leader, redirect
                        self.leader = cluster['leader']
                        logger.info("Redirected to %s:%s..." % 
                                            (self.leader, self.c.port))
                        raise NewLeader()
                    elif client_address in cluster['cluster']:
                        # Only report cluster once I'm a member
                        self.c.set.replace(set(cluster['cluster']))
            self.c.set.remove(self.leader)
            self._leader_election()
        except NewLeader:
            #self.manager.trigger_callback()
            if self.leader == client_address:
                self.c.is_leader = True
                self.c.promoted.set()
                self.stop() # doesn't work
            else:
                return

    def _server_keepalive(self, socket):
        return self.spawn_later(SERVER_KEEPALIVE_SECONDS, 
            lambda: socket.send('\n'))

    def _leader_election(self):
        candidates = list(self.c.set)
        candidates.sort()
        self.leader = candidates[0]
        logger.info("New leader %s:%s..." % (self.leader, self.c.port))
        # TODO: if i end up thinking i'm the leader when i'm not
        # then i will not rejoin the cluster
        raise NewLeader()
