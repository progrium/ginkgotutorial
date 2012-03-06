import random
import socket
import json
import socket
import collections
import logging

import gevent.pywsgi
import gevent.queue
import webob

from gservice.core import Service, autospawn
from gservice.config import Setting

logger = logging.getLogger(__name__)

class HttpStreamer(Service):
    address = Setting('pubsub_bind', default=('0.0.0.0', 8088))
    keepalive_interval = Setting('keepalive_interval', default=5)

    def __init__(self, hub):
        self.hub = hub

        self.add_service(
            gevent.pywsgi.WSGIServer(
                listener=self.address,
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

        self.hub.publish(request.path, str(request.body))

        start_response('200 OK', [
            ('Content-Type', 'text/plain')])
        return ["OK\n"]

    def handle_subscribe(self, env, start_response):
        request = webob.Request(env)
        subscription = self.hub.subscribe(request.path)
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


class HttpTailViewer(Service):
    address = Setting('tail_bind', default=('0.0.0.0', 8089))

    def __init__(self, hub):
        self.hub = hub

        self.add_service(
            gevent.pywsgi.WSGIServer(
                listener=self.address,
                application=self.handle,
                spawn=self.spawn,
                log=None))

        # This isn't the best we can do, but it makes things better
        self.catch(socket.error, lambda e,g: None)

    def handle(self, env, start_response):
        request = webob.Request(env)
        subscription = self.hub.subscribe(request.path)
        logger.info("New subscriber (tail view)")

        boundary = str(random.random())
        start_response('200 OK', [
            ('Content-Type', 'multipart/x-mixed-replace; boundary={}'.format(boundary)),
            ('Connection', 'keep-alive'),
            ('Cache-Control', 'no-cache, must-revalidate'),
            ('Expires', 'Tue, 11 Sep 1985 19:00:00 GMT'),])
        yield '--{}\n'.format(boundary)
        for msg in subscription:
            if msg is not None:
                yield '\n'.join([
                    'Content-Type: text/plain',
                    'Content-Length: {}'.format(len(msg)),
                    '\n{}'.format(msg),
                    '--{}\n'.format(boundary)])

