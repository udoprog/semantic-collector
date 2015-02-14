import os
import collections


class LinuxLoadAvg(object):
    PROC_LOADAVG = '/proc/loadavg'

    FIELDS = ['load1', 'load5', 'load10', 'proc', 'lastpid']

    loadavg = collections.namedtuple('loadavg', FIELDS)

    @classmethod
    def verify(cls):
        if not os.path.isfile(cls.PROC_LOADAVG):
            raise Exception('no such file: {0}'.format(cls.PROC_LOADAVG))

        return cls.read_loadavg()

    @classmethod
    def read_loadavg(cls):
        first = None

        with open(cls.PROC_LOADAVG, 'r') as f:
            first = f.readline().strip()

        p = first.split()

        if len(p) != len(cls.FIELDS):
            raise Exception("expected fields on first line")

        return cls.loadavg(float(p[0]), float(p[1]), float(p[2]), p[3], p[4])

    def __init__(self, registry):
        self.load1 = registry.metric(what='loadavg-1m')
        self.load5 = registry.metric(what='loadavg-5m')
        self.load10 = registry.metric(what='loadavg-10m')

    def __call__(self):
        l = self.read_loadavg()
        self.load1.update(l.load1)
        self.load5.update(l.load5)
        self.load10.update(l.load10)


def setup(scope):
    platform = scope.require('platform')

    if platform.is_linux():
        registry = scope.require('registry')
        LinuxLoadAvg.verify()
        return LinuxLoadAvg(registry)

    raise Exception('unsupported platform')
