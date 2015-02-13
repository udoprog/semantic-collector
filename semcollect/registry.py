import multiprocessing as mp


class Registry(object):
    class Metric(object):
        def __init__(self, value):
            self._v = value

        def update(self, value):
            self._v.value = value

    class State(object):
        def __init__(self, value):
            self._v = value

        def ok(self):
            self._v.value = 1

        def critical(self):
            self._v.value = 0

        def update(self, state):
            self._v.value = 1 if state else 0

    class Group(object):
        def __init__(self, registry):
            self._group = []
            self._registry = registry

        def metric(self, **tags):
            n, m = self._registry.metric(**tags)
            self._group.append(n)
            return m

        def state(self, **tags):
            n, m = self._registry.state(**tags)
            self._group.append(n)
            return m

        def _injectfree(self):
            for n in self._group:
                self._registry.free(n)

            self._group = []

        def _injectchild(self):
            return Registry.Group(self._registry)

    NaN = float('NaN')

    def __init__(self, **tags):
        self._p = 0
        self._vals = dict()
        self._states = dict()
        self._tags = dict()
        self._base = dict(tags)

    def _injectchild(self):
        return Registry.Group(self)

    def metric(self, **tags):
        n = self._p
        self._p += 1

        t = dict(self._base)
        t.update(tags)

        v = mp.Value('d', self.NaN)

        self._vals[n] = v
        self._tags[n] = t

        return n, Registry.Metric(v)

    def state(self, **tags):
        n = self._p
        self._p += 1

        t = dict(self._base)
        t.update(tags)

        v = mp.Value('b', 0)

        self._states[n] = v
        self._tags[n] = t

        return n, Registry.State(v)

    def free(self, n):
        self._vals.pop(n, None)
        self._states.pop(n, None)
        self._tags.pop(n, None)

    @property
    def values(self):
        return ((self._tags[n], v.value) for (n, v) in self._vals.items())

    @property
    def states(self):
        return ((self._tags[n], v.value == 1) for (n, v) in
                self._states.items())

    def update(self, n, value):
        self._vals[n].value = value
