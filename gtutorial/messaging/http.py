import random
import socket
import json
import socket
import collections
import logging

import gevent.pywsgi
import gevent.queue
import webob

from ginkgo.core import Service, autospawn
from ginkgo.config import Setting

logger = logging.getLogger(__name__)

class Subscription(gevent.queue.Queue):
    def __init__(self, channel):
        super(Subscription, self).__init__(maxsize=64)
        self.channel = channel

    def cancel(self):
        self.channel = None

class HttpStreamer(Service):
    port = Setting('pubsub_port', default=8088)
    keepalive_interval = Setting('keepalive_interval', default=5)

    def __init__(self):
        self.subscriptions = {}

        self.add_service(
            gevent.pywsgi.WSGIServer(
                listener=('0.0.0.0', self.port),
                application=self.handle,
                spawn=self.spawn,
                log=None))

        # This isn't the best we can do, but it makes things better
        self.catch(socket.error, lambda e,g: None)

    def handle(self, env, start_response):
        if env['REQUEST_METHOD'] == 'POST':
            return self.handle_publish(env, start_response)
        elif env['REQUEST_METHOD'] == 'GET':
            return self.handle_subscribe(env, start_response)
        else:
            start_response('405 Method not allowed', [])
            return ["Method not allowed\n"]

    def handle_publish(self, env, start_response):
        request = webob.Request(env)
        channel = request.path

        for subscription in self.subscriptions.get(channel, []):
            subscription.put(str(request.body))

        start_response('200 OK', [
            ('Content-Type', 'text/plain')])
        return ["OK\n"]

    def handle_subscribe(self, env, start_response):
        request = webob.Request(env)
        channel = request.path

        if channel not in self.subscriptions:
            self.subscriptions[channel] = []
        subscription = Subscription(channel)
        self.subscriptions[channel].append(subscription)

        self.keepalive(subscription)
        logger.info("New subscriber (stream)")

        start_response('200 OK', [
            ('Connection', 'keep-alive'),
            ('Cache-Control', 'no-cache, must-revalidate'),
            ('Expires', 'Tue, 11 Sep 1985 19:00:00 GMT'),])
        try:
            for msg in subscription:
                if msg is None:
                    yield '\n'
                else:
                    yield '{}\n'.format(msg)
        except:
            subscription.cancel()
            logger.info("Lost subscriber")

    @autospawn
    def keepalive(self, subscription):
        while subscription.channel is not None:
            subscription.put(None)
            gevent.sleep(self.keepalive_interval)


