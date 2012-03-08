import logging

import gevent

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

from .numbers import NumberClient
from .messaging.hub import MessageHub
from .coordination import Announcer
from .cluster import ClusterCoordinator

logger = logging.getLogger(__name__)

class NumberGateway(Service):
    identity = Setting('identity', default='127.0.0.1')
    leader = Setting('leader', default=None)

    def __init__(self):
        self.client = NumberClient(('127.0.0.1', 7776))
        self.cluster = ClusterCoordinator(self.identity, self.leader)
        self.hub = MessageHub(self.cluster.set, self.identity)
        self.announcer = Announcer(self.hub, self.cluster.set, self.identity)

        self.add_service(self.cluster)
        self.add_service(self.hub)
        self.add_service(self.announcer)
        self.add_service(self.client)

    def do_start(self):
        self._bridge()

    @autospawn
    def _bridge(self):
        for number in self.client:
            if self.cluster.is_leader:
                self.hub.publish('/numbers', number)
            gevent.sleep(0)
