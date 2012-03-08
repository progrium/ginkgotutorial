
def service():
    from gtutorial.messaging.hub import MessageHub
    from gtutorial.util import ObservableSet
    return MessageHub(ObservableSet(['127.0.0.1']))
