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

    # skip ram=1, and loop=7
    SKIP_MAJOR = set([1, 7])

    disk = collections.namedtuple('disk', DISK_STAT_FIELDS)

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
                major = int(parts[0])
                device = parts[2]
                rest = parts[3:]

                # disk block devices
                if major in cls.SKIP_MAJOR:
                    continue

                if len(rest) != len(cls.DISK_STAT_FIELDS):
                    raise Exception("expected DISK_STAT_FIELDS on first line")

                disks[device] = cls.disk(*(map(int, parts[3:])))

        return disks

    def __init__(self, registry, last, reload_latch):
        self.iostats = dict()
        self.reload_latch = reload_latch

        for device in last.keys():
            io = self.iostats[device] = dict()

            reg = registry.scoped(device=device)

            io['rd-op'] = reg.metric(
                what='io-read-operations', unit='operation/s')
            io['rd-merges'] = reg.metric(
                what='io-read-merges', unit='merge/s')
            io['rd-bytes'] = reg.metric(
                what='io-read-bytes', unit='B/s')
            io['rd-sectors'] = reg.metric(
                what='io-read-sectors', unit='sector/s')
            io['rd-await'] = reg.metric(
                what='io-read-await', unit='ms')

            io['wr-op'] = reg.metric(
                what='io-write-operations', unit='operation/s')
            io['wr-merges'] = reg.metric(
                what='io-write-merges', unit='merge/s')
            io['wr-bytes'] = reg.metric(
                what='io-write-bytes', unit='B/s')
            io['wr-sectors'] = reg.metric(
                what='io-write-sectors', unit='sector/s')
            io['wr-await'] = reg.metric(
                what='io-write-await', unit='ms')

            io['avqz'] = reg.metric(
                what='io-average-queue-size', unit='operation/s')
            io['util'] = reg.metric(
                what='io-utilization', unit='%')

        self.last_seen = set(last.keys())
        self.last_time = time.time()
        self.last = last

    def check_reload(self, disks):
        seen = set(disks.keys())

        if seen != self.last_seen:
            self.reload_latch()

        self.last_seen = seen

    def update(self, disks):
        now = time.time()
        diff = now - self.last_time
        self.last_time = now

        # not valid values can be set
        if diff <= 0:
            for io in self.iostats.values():
                for m in io.values():
                    m.unset()

            return

        for device, a in disks.items():
            io = self.iostats.get(device, None)
            b = self.last.get(device)

            if io is None or b is None:
                continue

            d = self.disk(*[(av - bv) / diff for av, bv in zip(a, b)])

            io['rd-op'].update(d.rd_ios)
            io['rd-merges'].update(d.rd_merges)
            io['rd-bytes'].update(d.rd_sectors * 512)
            io['rd-sectors'].update(d.rd_sectors)
            io['rd-await'].update(d.rd_ios)

            io['wr-op'].update(d.wr_ios)
            io['wr-merges'].update(d.wr_merges)
            io['wr-bytes'].update(d.wr_sectors * 512)
            io['wr-sectors'].update(d.wr_sectors)
            io['wr-await'].update(d.wr_ios)

            io['avqz'].update(d.rq_tics)
            io['util'].update(round(d.tot_tics / 1000, 2))

    def __call__(self):
        disks = self.read_disks()
        self.check_reload(disks)
        self.update(disks)
        self.last = disks


def setup(scope):
    #config = scope.require('config')
    platform = scope.require('platform')
    reload_latch = scope.require('reload')

    if platform.is_linux():
        registry = scope.require('registry')
        last = LinuxIOStat.verify()
        return LinuxIOStat(registry, last, reload_latch)

    raise Exception('unsupported platform')
