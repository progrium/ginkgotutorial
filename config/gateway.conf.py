import os

identity = os.environ.get('IDENTITY', '127.0.0.1')
leader = os.environ.get('LEADER')
cluster = os.environ.get('CLUSTER', '127.0.0.1').split(',')

def service():
    from gtutorial.gateway import NumberGateway
    return NumberGateway()
