import os
import collections
import time


class LinuxIOStat(object):
    PROC_DISKSTATS = '/proc/diskstats'

    DISK_STAT_FIELDS = [
        'rd_ios', 'rd_merges', 'rd_sectors', 'rd_tics',
        'wr_ios', 'wr_merges', 'wr_sectors', 'wr_tics',
        'ios_pgr', 'tot_tics', 'rq_tics'
    ]

    disk_stat = collections.namedtuple('disk_stat', DISK_STAT_FIELDS)

    @classmethod
    def verify(cls):
        if not os.path.isfile(cls.PROC_DISKSTATS):
            raise Exception('no such file: {0}'.format(cls.PROC_DISKSTATS))

        return cls.read_disks()

    @classmethod
    def read_disks(cls):
        disks = dict()

        with open(cls.PROC_DISKSTATS, 'r') as f:
            for line in f:
                line = line.strip()
                parts = line.split()
                device = parts[2]
                rest = parts[3:]

                if len(rest) != len(cls.DISK_STAT_FIELDS):
                    raise Exception("expected DISK_STAT_FIELDS on first line")

                disks[device] = cls.disk_stat(*(map(int, parts[3:])))

        return disks

    def __init__(self, registry, last):
        self.iostats = dict()

        for device in last.keys():
            for field in self.DISK_STAT_FIELDS:
                self.iostats[(device, field)] = registry.metric(
                    what='iostat-{0}'.format(field.replace('_', '-')),
                    device=device)

        self.last_time = time.time()
        self.last = last

    def __call__(self):
        now = time.time()
        diff = now - self.last_time
        self.last_time = now

        if diff <= 0:
            return

        s = self.read_disks()

        for device, s1 in s.items():
            s2 = self.last.get(device)

            if s2 is None:
                continue

            d = self.disk_stat(
                *((a - b) / diff for (a, b) in zip(s1, s2)))

            for v, field in zip(d, self.DISK_STAT_FIELDS):
                try:
                    m = self.iostats[(device, field)]
                except KeyError:
                    continue

                m.update(v)

        self.last = s


def setup(scope):
    #config = scope.require('config')
    platform = scope.require('platform')

    if platform.is_linux():
        registry = scope.require('registry')
        last = LinuxIOStat.verify()
        return LinuxIOStat(registry, last)

    raise Exception('unsupported platform')
