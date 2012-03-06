
def service():
    from gtutorial.messaging import MessageHub
    import sys, logging
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)
    return MessageHub()
