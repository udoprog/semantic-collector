import logging
import signal
import sys
import os.path
import multiprocessing as mp

log = logging.getLogger(__name__)


class Collector(object):
    class Instance(object):
        def __init__(self, name, p, pipe, stat, injector, c):
            # if not None, the current running process.
            self._name = name
            self._p = p
            self._pipe = pipe
            self._stat = stat
            self._injector = injector
            self._c = c
            self._runs = 0
            self._errors = 0

        def stat(self):
            return self._stat

        def is_alive(self):
            return self._p.is_alive()

        def collect(self, i):
            self._pipe.send(i)
            self._runs += 1
            return i

        def errored(self, count=1):
            self._errors += count

        def terminate(self, graceful=False):
            if graceful:
                log.info("%s: terminate (graceful)", self)
                self._pipe.send(None)
                self._p.join(self._c.graceful_timeout)
            else:
                log.info("%s: terminate (forced)", self)

            attempt = 0

            while self._p.exitcode is None:
                if attempt >= self._c.max_forceful_attempts:
                    raise Exception(
                        ('{0}: could not be terminated '
                         'after %d attempts').format(
                             self, self._c.max_forceful_attempts))

                log.warn('%s: terminate (attempt %d of %d)', self, attempt,
                         self._c.max_forceful_attempts)
                self._p.terminate()
                self._p.join(self._c.forceful_timeout)
                attempt += 1

            log.info("%s: exited=%d", self.__str__(), self._p.exitcode)

            self._injector.free()
            self._pipe.close()

        def needs_recycling(self):
            max_runs = (
                self._c.max_runs is not None and self._runs > self._c.max_runs)
            max_errors = (
                self._c.max_errors is not None and
                self._errors > self._c.max_errors)

            return (max_runs or max_errors)

        def __str__(self):
            return "{0}:{1}".format(self._name, self._p.pid)

    def __init__(self, path, name, out, injector, instance_config):
        self._path = path
        self._name = name
        self._out = out
        self._injector = injector
        self._instance_config = instance_config
        self._inst = None

    def errored(self, count=1):
        self._inst.errored(count)

    def collect(self, i):
        if self._inst is None:
            self._inst = self._start()

        if not self._inst.is_alive():
            log.error('%s: no longer alive, restarting',
                      self._inst)
            self.restart(False)

        if self._is_outdated() or self._inst.needs_recycling():
            log.info('%s: recycling', self._inst)
            self.restart(True)

        return self._inst.collect(i)

    def restart(self, graceful=False):
        if self._inst is None:
            raise Exception('instance not running')

        self._inst.terminate(graceful)
        self._inst = self._start()

    def stop(self):
        """
        Stop the collector.
        """
        if self._inst is None:
            raise Exception('{0}: no instance running'.format(self))

        self._inst.terminate(True)
        self._inst = None
        self._injector.free()

    def _is_outdated(self):
        """
        Checks stat on the process file to see if it is newer than when the
        process was loaded.

        @return True if the current collector instance is outdated, False
                otherwise.
        """
        return self._inst.stat() != limited_stat(self._path)

    def _compile(self):
        scope = dict()

        with open(self._path) as f:
            code = compile(f.read(), self._path, 'exec')
            exec(code, scope)

        return scope

    def _start(self):
        log.info('%s: starting', self)

        stat = limited_stat(self._path)

        scope = self._compile()

        setup = scope.get('setup', None)

        if setup is None:
            raise Exception('{0}: no #setup method found'.format(self._path))

        injector = self._injector.child()
        collect = setup(injector)

        if collect is None:
            raise Exception(
                '{0}: #setup must not return None'.format(self._path))

        start = getattr(collect, 'start', None)
        stop = getattr(collect, 'stop', None)

        inp, out = mp.Pipe(False)

        p = mp.Process(target=instance_loop,
                       args=(self._name, inp, self._out, start, stop, collect),
                       name=self._name)
        p.start()

        return Collector.Instance(self._name, p, out, stat, injector,
                                  self._instance_config)

    def __str__(self):
        if self._inst is not None:
            return str(self._inst)

        return '{0}:<no instance>'.format(self._name)


def limited_stat(path):
    """
    Perform a stat that only includes size and last modification time.
    """
    s = os.stat(path)
    return (s.st_size, s.st_mtime)


def instance_loop(name, inp, out, start, stop, collect):
    """
    Process loop for a single instance.
    """
    name = "{0}:{1}".format(name, os.getpid())

    def _handle_term(sig, frame):
        log.warn("%s: terminating (by signal)", name)
        sys.exit(1)

    # Handle SIGTERM because it signals a forced terminate by manager process.
    signal.signal(signal.SIGTERM, _handle_term)

    if start is not None:
        try:
            start()
        except:
            log.error('%s: failed to start', name, exc_info=sys.exc_info())
            sys.exit(1)

    while True:
        try:
            i = inp.recv()
        except:
            log.error('%s: receive failed', name,
                      exc_info=sys.exc_info())
            break

        if i is None:
            break

        try:
            collect()
        except Exception as e:
            log.error('%s: collector failed: %s', name, e)
            out.put((i, False))
        else:
            out.put((i, True))

    if stop is not None:
        try:
            stop()
        except:
            log.error('%s: failed to stop', name, exc_info=sys.exc_info())

    sys.exit(0)
