
from gevent.queue import Queue
from gevent.socket import create_connection
from gservice.core import Service, autospawn
from gservice.config import Setting

class NumberClient(Service):
    def __init__(self, address=None):
        self.address = address
        self.queue = Queue()
        self.socket = None

    def do_start(self):
        self._connect()

    @autospawn
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

