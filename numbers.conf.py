rate_per_minute = 320

def service():
    from gtutorial.server.numbers import NumberServer
    import sys, logging
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)
    return NumberServer()
