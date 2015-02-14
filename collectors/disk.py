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
            disks.append((m.fs_spec, m.fs_file, cls.read_disk(m.fs_file)))

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

    def __init__(self, registry, disks, reload_latch):
        self.last = disks
        self.reload_latch = reload_latch
        self.disks = dict()
        self.b = dict()
        self.last_seen = set(f for (_, f, _) in disks)

        for (device, f, d) in disks:
            if d.total <= 0:
                continue

            disk = self.disks[f] = dict()

            disk['total'] = registry.metric(
                what='disk-total', mountpoint=f, device=device, unit='B')
            disk['free'] = registry.metric(
                what='disk-free', mountpoint=f, device=device, unit='B')
            disk['avail'] = registry.metric(
                what='disk-avail', mountpoint=f, device=device, unit='B')
            disk['rest'] = registry.metric(
                what='disk-rest', mountpoint=f, device=device, unit='B')
            disk['free-perc'] = registry.metric(
                what='disk-free-percentage', mountpoint=f, device=device,
                unit='%')
            disk['avail-perc'] = registry.metric(
                what='disk-avail-percentage', mountpoint=f, device=device,
                unit='%')
            disk['rest-perc'] = registry.metric(
                what='disk-rest-percentage', mountpoint=f, device=device,
                unit='%')

        self.update(disks)

    def update(self, disks):
        for (device, f, d) in disks:
            if d.total <= 0:
                continue

            disk = self.disks.get(f, None)

            if disk is None:
                continue

            total = float(d.total)
            free = float(d.free)
            avail = float(d.avail)
            rest = float(d.rest)

            disk['total'].update(total)
            disk['free'].update(free)
            disk['avail'].update(avail)
            disk['rest'].update(rest)
            disk['free-perc'].update(round(free / total, 2))
            disk['avail-perc'].update(round(avail / total, 2))
            disk['rest-perc'].update(round(rest / total, 2))

    def check_reload(self, disks):
        # disk layout has change, ask to be reloaded.
        seen = set(f for (_, f, _) in disks)

        if seen != self.last_seen:
            self.reload_latch()

        self.last_seen = seen

    def __call__(self):
        disks = self.read_disks()
        self.check_reload(disks)
        self.update(disks)


def setup(scope):
    #config = scope.require('config')
    platform = scope.require('platform')
    reload_latch = scope.require('reload')

    if platform.is_linux():
        registry = scope.require('registry')
        disks = LinuxDisk.verify()
        return LinuxDisk(registry, disks, reload_latch)

    raise Exception('unsupported platform')
