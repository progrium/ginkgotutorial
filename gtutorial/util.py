class Observable(object):
    def __init__(self):
        self._observers = []

    def attach(self, observer):
        if not observer in self._observers:
            self._observers.append(observer)

    def detach(self, observer):
        try:
            self._observers.remove(observer)
        except ValueError:
            pass

    def notify(self, *args, **kwargs):
        for observer in self._observers:
            if hasattr(observer, '__call__'):
                observer(*args, **kwargs)

class ObservableSet(Observable):
    def __init__(self):
        super(ObservableSet, self).__init__()
        self._set = set()

    def add(self, element):
        if element not in self._set:
            self._set.add(element)
            self.notify(add=element)

    def remove(self, element):
        if element in self._set:
            self._set.remove(element)
            self.notify(remove=element)

    def replace(self, new_set):
        added = new_set - self._set
        removed = self._set - new_set
        self._set = new_set
        for element in added:
            self.notify(add=element)
        for element in removed:
            self.notify(remove=element)

    def __iter__(self):
        return self._set.__iter__()

    def __repr__(self):
        return str(self._set)

