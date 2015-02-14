import logging
import signal
import sys
import os.path
import multiprocessing as mp

log = logging.getLogger(__name__)


class Collector(object):
    class Latch(object):
        def __init__(self):
            self._b = mp.Value('b', 0)

        def __call__(self):
            self._b.value = 1

        def is_set(self):
            return self._b.value != 0

    class Instance(object):
        def __init__(self, path, name, process, pipe,
                     injector, reload_latch, config):
            # if not None, the current running process.
            self._path = path
            self._name = name
            self._process = process
            self._pipe = pipe
            self._injector = injector
            self._reload_latch = reload_latch
            self._c = config
            self._stat = self._stat_path()
            self._runs = 0
            self._errors = 0

        def is_alive(self):
            return self._process.is_alive()

        def collect(self, i):
            self._pipe.send(i)
            self._runs += 1

        def errored(self, count=1):
            self._errors += count

        def terminate(self, graceful=False):
            if graceful:
                log.info("%s: terminate (graceful)", self)
                self._pipe.send(None)
                self._process.join(self._c.graceful_timeout)
            else:
                log.info("%s: terminate (forced)", self)

            attempt = 0

            while self._process.exitcode is None:
                if attempt >= self._c.max_forceful_attempts:
                    raise Exception(
                        ('{0}: could not be terminated '
                         'after %d attempts').format(
                             self, self._c.max_forceful_attempts))

                log.warn('%s: terminate (attempt %d of %d)', self, attempt,
                         self._c.max_forceful_attempts)
                self._process.terminate()
                self._process.join(self._c.forceful_timeout)
                attempt += 1

            log.info("%s: exited=%d", self.__str__(), self._process.exitcode)

            self._injector.free()
            self._pipe.close()

        def needs_recycling(self):
            return any(r is not None for r in self.reasons())

        def reasons(self):
            """
            Return a list of reasons for why this instance should be recycled.
            """
            if self._stat != self._stat_path():
                yield 'source updated'

            if self._c.max_runs is not None and \
               self._runs > self._c.max_runs:
                yield 'run limit'

            if self._c.max_errors is not None and \
               self._errors > self._c.max_errors:
                yield 'error limit'

            if self._reload_latch.is_set():
                yield 'reloaded'

        def _stat_path(self):
            """
            Perform a stat that only includes size and last modification time.
            """
            s = os.stat(self._path)
            return (s.st_size, s.st_mtime)

        def __str__(self):
            return "{0}:{1}".format(self._name, self._process.pid)

    def __init__(self, path, name, out, injector, instance_config):
        self._path = path
        self._name = name
        self._out = out
        self._injector = injector
        self._instance_config = instance_config
        self._instance = None
        self._failed_restart_timer = 0

    def errored(self, count=1):
        self._instance.errored(count)

    def check(self):
        self._check_instance()

    def collect(self, i):
        self._check_instance()
        self._instance.collect(i)

    def soft_restart(self, graceful=False):
        """
        A restart implementation that tries to keep the old instance alive
        until a new one has come up.

        It also implements back-off using a timer to avoid trying to restart a
        broken collector too often.
        """

        # do not restart while timer is active
        if self._failed_restart_timer > 0:
            self._failed_restart_timer -= 1
            return

        try:
            new_instance = self._new_instance()
        except:
            self._failed_restart_timer = 10
            log.error('%s: failed to restart', self, exc_info=sys.exc_info())
            return

        self._instance.terminate(graceful)
        self._instance = new_instance

    def restart(self, graceful=False):
        if self._instance is not None:
            self._instance.terminate(graceful)
            self._instance = None

        self._instance = self._new_instance()

    def stop(self, graceful=True):
        """
        Stop the collector.
        """
        if self._instance is None:
            return

        self._instance.terminate(graceful)
        self._instance = None

    def _check_instance(self):
        if self._instance is None:
            self._instance = self._new_instance()

        if not self._instance.is_alive():
            log.error('%s: no longer alive, restarting',
                      self._instance)
            self.restart(False)

        if self._instance.needs_recycling():
            log.info('%s: recycling (%s)', self._instance,
                     ', '.join(self._instance.reasons()))
            self.soft_restart(True)

    def _compile(self):
        scope = dict()

        with open(self._path) as f:
            code = compile(f.read(), self._path, 'exec')
            exec(code, scope)

        return scope

    def _new_instance(self):
        scope = self._compile()

        setup = scope.get('setup', None)

        if setup is None:
            raise Exception('{0}: no #setup method found'.format(self._path))

        reload_latch = Collector.Latch()

        injector = self._injector.child(dict(reload=reload_latch))

        try:
            collect = setup(injector)
        except:
            injector.free()
            raise

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

        return Collector.Instance(
            self._path, self._name, p, out, injector, reload_latch,
            self._instance_config)

    def __str__(self):
        if self._instance is not None:
            return str(self._instance)

        return '{0}:<no instance>'.format(self._name)


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
