
def service():
    from gtutorial.messaging.hub import MessageHub
    from gtutorial.util import ObservableSet
    import sys, logging
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)
    return MessageHub(ObservableSet(['127.0.0.1']))
