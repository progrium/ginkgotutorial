def service():
    from gtutorial.bridge import NumberWebBridge
    import sys, logging
    logging.basicConfig(
        format="%(asctime)s %(levelname) 7s %(module)s: %(message)s",
        stream=sys.stdout,
        level=logging.DEBUG)
    return NumberWebBridge()
