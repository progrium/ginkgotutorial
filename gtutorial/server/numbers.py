import sys
import random
import logging

import gevent
from gevent.server import StreamServer
from gservice.core import Service
from gservice.config import Setting

logger = logging.getLogger(__name__)

class NumberServer(Service):
    address = Setting("numbers_bind", default=('0.0.0.0', 7776))
    emit_rate = Setting("rate_per_minute", default=60)

    def __init__(self):
        self.add_service(
                StreamServer(self.address, self.handle))

    def handle(self, socket, address):
        logger.debug("New connection {}".format(address))
        while True:
            try:
                number = random.randint(0, 10)
                socket.send("{}\n".format(number))
                gevent.sleep(60.0/self.emit_rate)
            except IOError:
                logger.debug("Connection dropped {}".format(address))
                break
