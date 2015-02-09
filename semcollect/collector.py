import os.path
import logging
import sys
import multiprocessing as mp

log = logging.getLogger(__name__)


class Collector(object):
    class Instance(object):
        def __init__(self, name, p, pipe, **kw):
            # if not None, the current running process.
            self._name = name
            self._p = p
            self._pipe = pipe
            self._runs = 0
            self._errors = 0
            # number of collections until the process will be recycled
            self._max_runs = kw.get('max_runs', 10000)
            # number of errors allowed until the process will be recycled
            self._max_errors = kw.get('max_errors', 5)
            # allowed timeout for a graceful shutdown to occur.
            self._graceful_timeout = kw.get('graceful_timeout', 2.0)
            # allowed timeout for a forceful shutdown to occur, failure to
            # perform a forceful shutdown will crash the collector.
            self._forceful_timeout = kw.get('forceful_timeout', 2.0)
            # maximum number of forceful attempts allowed.
            self._max_forceful_attempts = kw.get('max_forceful_attempts', 5)

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
                self._p.join(self._graceful_timeout)
            else:
                log.info("%s: terminate (forced)", self)

            attempt = 0

            while self._p.exitcode is None:
                if attempt >= self._max_forceful_attempts:
                    raise Exception(
                        ('{0}: could not be terminated '
                         'after %d attempts').format(
                             self, self._max_forceful_attempts))

                log.warn('%s: terminate (attempt %d of %d)', self, attempt,
                         self._max_forceful_attempts)
                self._p.terminate()
                self._p.join(self._forceful_timeout)
                attempt += 1

            log.info("%s: exited=%d", self.__str__(), self._p.exitcode)
            self._runs = 0
            self._errors = 0
            self._pipe.close()

        def needs_recycling(self):
            max_runs = (
                self._max_runs is not None and self._runs > self._max_runs)
            max_errors = (
                self._max_errors is not None and
                self._errors > self._max_errors)

            return (max_runs or max_errors)

        def __str__(self):
            return "{0}:{1}".format(self._name, self._p.pid)

    def __init__(self, name, out, scope, collect, **kw):
        self._name = name
        self._out = out
        self._scope = scope
        self._collect = collect
        self._inst = None
        self.instance_kw = kw

    def errored(self, count=1):
        self._inst.errored(count)

    def collect(self, i):
        if self._inst is None:
            raise Exception('instance not running')

        if not self._inst.is_alive():
            log.error('%s: no longer alive, restarting',
                      self._inst)
            self.restart(False)

        if self._inst.needs_recycling():
            log.info('%s: recycling', self._inst)
            self.restart(True)

        return self._inst.collect(i)

    def restart(self, graceful=False):
        if self._inst is None:
            raise Exception('instance not running')

        self._inst.terminate(graceful)
        self._inst = self._instance()

    def start(self):
        if self._inst is not None:
            raise Exception('instance already running')

        self._inst = self._instance()

    def stop(self):
        """
        Stop the collector.
        """
        if self._inst is None:
            raise Exception('instance not running')

        self._scope.free()
        self._inst.terminate(True)
        self._inst = None

    def _instance(self):
        inp, out = mp.Pipe(False)

        p = mp.Process(target=instance_loop,
                       args=(self._name, inp, self._out, self._collect),
                       name=self._name)
        p.start()

        return Collector.Instance(self._name, p, out, **self.instance_kw)

    @classmethod
    def load(cls, path, out, scope, **kw):
        """
        Load a collector from the given path.
        """

        log.info('%s: loading', path)

        lcl = dict()

        with open(path) as f:
            code = compile(f.read(), path, 'exec')
            exec(code, lcl)

        setup = lcl.get('setup', None)

        if setup is None:
            raise Exception('{0}: no #setup method found'.format(path))

        collect = setup(scope)

        if collect is None:
            return None

        name = os.path.basename(path)
        return cls(name, out, scope, collect, **kw)

    def __str__(self):
        if self._inst is not None:
            return str(self._inst)

        return '{0}:<no instance>'.format(self._name)


def instance_loop(name, inp, out, collect):
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
        except:
            log.error('collector failed', exc_info=sys.exc_info())
            out.put((i, False))
        else:
            out.put((i, True))

    sys.exit(0)
