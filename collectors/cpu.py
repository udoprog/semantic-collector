import os
import collections


class LinuxCPU(object):
    PROC_STAT = '/proc/stat'

    FIELDS = [
        'user', 'nice', 'system', 'idle', 'iowait',
        'irq', 'softirq', 'steal', 'guest', 'guest_nice']

    cpu_stat = collections.namedtuple('cpu_stat', FIELDS)

    @classmethod
    def verify(cls):
        if not os.path.isfile(cls.PROC_STAT):
            raise Exception('no such file: {0}'.format(cls.PROC_STAT))

        return cls.read_cpu()

    @classmethod
    def read_cpu(cls):
        first = None

        with open(cls.PROC_STAT, 'r') as f:
            first = f.readline().strip()

        parts = first.split()

        if parts[0] != 'cpu':
            raise Exception("invalid first line, expected 'cpu'")

        rest = parts[1:]

        if len(rest) != 10:
            raise Exception("expected 10 fields on first line")

        return cls.cpu_stat(*(int(p) for p in rest))

    def __init__(self, registry, last):
        self.cpu_usages = dict()

        for field in self.FIELDS:
            self.cpu_usages[field] = registry.metric(
                what='cpu-usage-{0}'.format(field.replace('_', '-')),
                unit='%')

        self.last = last

    def __call__(self):
        s = self.read_cpu()
        diff = sum(s) - sum(self.last)

        if diff <= 0:
            return

        diff = float(diff)
        values = (round((a - b) / diff, 2) for (a, b) in zip(s, self.last))

        for v, f in zip(values, self.FIELDS):
            self.cpu_usages[f].update(v)

        self.last = s


def setup(scope):
    #config = scope.require('config')
    platform = scope.require('platform')

    if platform.is_linux():
        registry = scope.require('registry')
        last = LinuxCPU.verify()
        return LinuxCPU(registry, last)

    raise Exception('unsupported platform')
