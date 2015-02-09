import multiprocessing as mp


class Registry(object):
    class Metric(object):
        def __init__(self, value):
            self._v = value

        def update(self, value):
            self._v.value = value

    class Group(object):
        def __init__(self, registry):
            self._group = []
            self._registry = registry

        def metric(self, **tags):
            n, m = self._registry.metric(**tags)
            self._group.append(n)
            return m

        def _injectfree(self):
            for n in self._group:
                self._registry.free(n)

            self._group = []

    NaN = float('NaN')

    def __init__(self, **tags):
        self._p = 0
        self._vals = dict()
        self._tags = dict()
        self._base = dict(tags)

    def group(self):
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

    def free(self, n):
        self._vals.pop(n, None)
        self._tags.pop(n, None)

    @property
    def values(self):
        return ((self._tags[n], v.value) for (n, v) in self._vals.items())

    def update(self, n, value):
        self._vals[n].value = value
