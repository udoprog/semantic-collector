import os
import collections


class LinuxDisk(object):
    PROC_MOUNTS = '/proc/mounts'

    MOUNT_FIELDS = [
        'fs_spec', 'fs_file', 'fs_vfstype',
        'fs_mntopts', 'fs_freq', 'fs_passno'
    ]

    DISK_FIELDS = ['total', 'free', 'avail', 'rest']

    SKIP_FSTYPE = set([
        'cgroup', 'devpts', 'sysfs', 'proc',
        'devtmpfs', 'mqueue', 'autofs'])

    mount = collections.namedtuple('mount', MOUNT_FIELDS)
    disk = collections.namedtuple('disk', DISK_FIELDS)

    @classmethod
    def verify(cls):
        if not os.path.isfile(cls.PROC_MOUNTS):
            raise Exception('no such file: {0}'.format(cls.PROC_MOUNTS))

        return cls.read_disks()

    @classmethod
    def read_disks(cls):
        disks = list()

        for m in cls.read_mounts():
            disks.append((m.fs_file, cls.read_disk(m.fs_file)))

        return disks

    @classmethod
    def read_mounts(cls):
        mounts = list()

        with open(cls.PROC_MOUNTS, 'r') as f:
            for line in f:
                m = line.strip().split()

                if len(m) != 6:
                    raise Exception('Invalid mount')

                mount = cls.mount(*m)

                if mount.fs_spec == mount.fs_vfstype:
                    continue

                if mount.fs_vfstype in cls.SKIP_FSTYPE:
                    continue

                mounts.append(mount)

        return mounts

    @classmethod
    def read_disk(cls, file):
        s = os.statvfs(file)
        total = s.f_frsize * s.f_blocks
        free = s.f_frsize * s.f_bfree
        avail = s.f_frsize * s.f_bavail
        rest = free - avail
        return cls.disk(total, free, avail, rest)

    def __init__(self, registry, disks):
        self.last = disks
        self.p = dict()
        self.b = dict()

        for (f, d) in disks:
            self.b[(f, 'total')] = registry.metric(
                what='disk-total', disk=f, unit='B')
            self.b[(f, 'free')] = registry.metric(
                what='disk-free', disk=f, unit='B')
            self.b[(f, 'avail')] = registry.metric(
                what='disk-avail', disk=f, unit='B')
            self.b[(f, 'rest')] = registry.metric(
                what='disk-rest', disk=f, unit='B')
            self.p[(f, 'free')] = registry.metric(
                what='disk-free-percentage', disk=f, unit='%')
            self.p[(f, 'avail')] = registry.metric(
                what='disk-avail-percentage', disk=f, unit='%')
            self.p[(f, 'rest')] = registry.metric(
                what='disk-rest-percentage', disk=f, unit='%')

        self.update(disks)

    def update(self, disks):
        for (f, d) in disks:
            if d.total <= 0:
                continue

            total = float(d.total)
            free = float(d.free)
            avail = float(d.avail)
            rest = float(d.rest)

            self.b[(f, 'total')].update(total)
            self.b[(f, 'free')].update(free)
            self.b[(f, 'avail')].update(avail)
            self.b[(f, 'rest')].update(rest)
            self.p[(f, 'free')].update(round(free / total, 2))
            self.p[(f, 'avail')].update(round(avail / total, 2))
            self.p[(f, 'rest')].update(round(rest / total, 2))

    def __call__(self):
        disks = self.read_disks()
        self.update(disks)


def setup(scope):
    #config = scope.require('config')
    platform = scope.require('platform')

    if platform.is_linux():
        registry = scope.require('registry')
        disks = LinuxDisk.verify()
        return LinuxDisk(registry, disks)

    raise Exception('unsupported platform')
