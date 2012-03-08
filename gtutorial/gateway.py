import logging

import gevent

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

from .numbers import NumberClient
from .messaging.hub import MessageHub
from .coordination import Announcer
from .coordination import Leadership
from .util import ObservableSet

logger = logging.getLogger(__name__)

class NumberGateway(Service):
    identity = Setting('identity', default='127.0.0.1')
    cluster_ = Setting('cluster', default=['127.0.0.1'])

    def __init__(self):
        self.client = NumberClient(('127.0.0.1', 7776))
        self.cluster = Leadership(self.identity, ObservableSet(self.cluster_))
        self.hub = MessageHub(self.cluster.set, self.identity)
        self.announcer = Announcer(self.hub, self.cluster)

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
