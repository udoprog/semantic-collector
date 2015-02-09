import platform


class Platform(object):
    def is_linux(self):
        return platform.system().lower() == 'linux'

    def is_darwin(self):
        return platform.system().lower() == 'darwin'
