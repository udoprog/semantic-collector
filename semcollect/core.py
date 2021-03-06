import multiprocessing as mp
import logging
import yaml
import os.path
import sys
import time
import queue

from .registry import Registry
from .injector import Injector
from .platform import Platform
from .collector import Collector
from .config import Root, ConfigException

log = logging.getLogger(__name__)

TASK_MOD = 2 ** 20


class Core(object):
    def __init__(self, **kw):
        self._timeout = kw.get('timeout', 10)
        self._interval = kw.get('interval', 30)
        self._backoff = kw.get('backoff', 10)
        self._config_path = kw.get('config', None)
        self._collector_paths = kw.get('collectors', [])
        self._out = mp.Queue()
        self._collectors = None
        self._registry = None
        self._signalled = False
        self._taskid = 0

    def signalled(self):
        self._signalled = True

    def setup(self):
        self._collectors, self._registry = self._setup()

    def stop(self):
        for c in self._collectors:
            c.stop()

    def reload(self):
        log.info('reloading collectors')

        try:
            collectors, registry = self._setup()
        except:
            log.error('reload failed', exc_info=sys.exc_info())
        else:
            for c in self._collectors:
                log.debug('%s: deallocating', c)
                c.stop()

            self._collectors = collectors
            self._registry = registry

    def check_collectors(self):
        for c in self._collectors:
            try:
                c.check()
            except:
                log.error('%s: failed to check', c, exc_info=sys.exc_info())

    def collect_all(self):
        collects = dict()

        for c in self._collectors:
            i = self._taskid
            self._taskid = (self._taskid + 1) % TASK_MOD

            try:
                c.collect(i)
            except Exception:
                log.error('%s: failed to collect', c, exc_info=sys.exc_info())
                continue

            collects[i] = c

        time_left = self._timeout
        then = time.time()

        while len(collects) > 0:
            if time_left <= 0:
                break

            try:
                i, ok = self._out.get(True, time_left)
            except queue.Empty:
                # timeout
                break

            now = time.time()
            time_left -= (now - then)
            then = now

            c = collects.pop(i, None)

            if c is None:
                log.error('no collector associated with id %d', i)
                continue

            # mark collector as errored.
            if not ok:
                c.errored()

        # restart collectors that did not finish in time
        for i, c in collects.items():
            log.warn('%s: timeout (task %d)', c, i)
            c.restart()

        # empty output queue for straggling processes.
        # at this point, all processes which were not part of the current
        # collection must be dead.
        while True:
            try:
                self._out.get_nowait()
            except queue.Empty:
                break

    def run_once(self):
        self._signalled = False

        next_run = (time.time() + self._interval)

        self.collect_all()

        if self._signalled:
            return

        if log.isEnabledFor(logging.DEBUG):
            log.debug("%d value(s)", sum(1 for i in self._registry.values))
            log.debug("%d state(s)", sum(1 for i in self._registry.states))

        diff = next_run - time.time()

        if diff > 0:
            log.debug('sleep: %0.2fs', diff)

            while diff > 0:
                time.sleep(1.0)

                if self._signalled:
                    return

                self.check_collectors()
                diff = min(next_run - time.time(), diff - 1.0)

            # sleep the rest of the time if necessary
            tail = next_run - time.time()

            if tail > 0:
                log.debug('tail=%0.2fs', tail)
                time.sleep(tail)

            return

        log.warn("Run took %0.2fs too long :(, sleeping %ds",
                 -diff, self._backoff)
        time.sleep(self._backoff)

    def _setup(self):
        config = load_config(self._config_path)

        root = None

        try:
            root = Root.load(config)
        except ConfigException as e:
            log.error('%s: invalid: %s', self._config_path, e)

        if root is None:
            raise Exception('{0}: could not load configuration'.format(
                self._config_path))

        registry = Registry(**config.get('tags', {}))

        components = dict(platform=Platform(), registry=registry)
        injector = Injector(components)

        known = self._load_collectors()

        collectors = self._build_collectors(known, root, injector)

        return collectors, registry

    def _build_collectors(self, known, root, injector):
        collectors = []

        for c in root.collectors:
            path = known.get(c.type, None)

            if path is None:
                raise Exception(
                    "'{0}' is not a known collector type".format(c.type))

            child = injector.child(dict(config=c.config))
            collector = Collector(
                path, c.type, self._out, child, root.instance_config)
            collectors.append(collector)

        return collectors

    def _load_collectors(self):
        collectors = dict()

        for p in self._collector_paths:
            if not os.path.isdir(p):
                continue

            for n in os.listdir(p):
                if n.startswith('.') or not n.endswith('.py'):
                    continue

                path = os.path.join(p, n)
                name, _ = os.path.splitext(n)

                collectors[name] = path

        return collectors


def load_config(path):
    if path is None or not os.path.isfile(path):
        return {}

    with open(path) as f:
        return yaml.load(f)
