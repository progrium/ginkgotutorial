def service():
    from gtutorial.gateway import NumberGateway
    import sys, logging
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)
    return NumberGateway()
