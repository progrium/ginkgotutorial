import sys
import random
import logging

import gevent
from gevent.server import StreamServer
from gevent.queue import Queue
from gevent.socket import create_connection

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

logger = logging.getLogger(__name__)

class NumberServer(Service):
    address = Setting("numbers_bind", default=('0.0.0.0', 7776))
    emit_rate = Setting("rate_per_minute", default=60)

    def __init__(self):
        self.add_service(
                StreamServer(self.address, self.handle))

    def do_start(self):
        logger.info("NumberServer is starting.")

    def do_stop(self):
        logger.info("NumberServer is stopping.")

    def do_reload(self):
        logger.info("NumberServer is reloading.")

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

class NumberClient(Service):
    def __init__(self, address=None):
        self.address = address
        self.queue = Queue()
        self.socket = None

    def do_start(self):
        self.spawn(self._connect)

    def _connect(self):
        if self.socket is not None:
            raise RuntimeError("Client is already connected")
        self.socket = create_connection(self.address)
        fileobj = self.socket.makefile()
        while True:
            try:
                number = fileobj.readline() # returns None on EOF
                if number is not None:
                    self.queue.put(number.strip())
                else:
                    break
            except IOError:
                break
        self.socket = None

    def __iter__(self):
        return self

    def next(self):
        return self.queue.get()

