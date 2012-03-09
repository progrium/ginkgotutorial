
numbers_bind = ('0.0.0.0', 7776)
rate_per_minute = 60

def service():
    from gtutorial.numbers import NumberServer
    return NumberServer()
