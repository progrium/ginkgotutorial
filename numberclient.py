from gtutorial.client.numbers import NumberClient

try:
    client = NumberClient(('127.0.0.1', 7776))
    with client:
        for number in client:
            print number
except KeyboardInterrupt:
    pass
