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

log = logging.getLogger(__name__)

TASK_MOD = 2 ** 20

POLICY_ALL = 1
POLICY_CONFIGURED = 2

collector_policies = {
    'configured': POLICY_CONFIGURED,
    'all': POLICY_ALL
}


class Core(object):
    def __init__(self, ns):
        self._ns = ns
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

    def run_all(self):
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

        time_left = self._ns.timeout
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

        next_run = (time.time() + self._ns.interval)

        self.run_all()

        if self._signalled:
            return

        if log.isEnabledFor(logging.DEBUG):
            for tags, v in self._registry.values:
                log.debug("%s: %0.2f", tags, v)

            for tags, v in self._registry.states:
                log.debug("%s: %s", tags, v)

        diff = next_run - time.time()

        if diff > 0:
            log.debug("sleep: %0.2fs", diff)
            time.sleep(diff)
            return

        log.warn("Run took %0.2fs too long :(, sleeping %ds",
                 -diff, self._ns.backoff)
        time.sleep(self._ns.backoff)

    def _setup(self):
        config = load_config(self._ns.config)

        configured = config.get('collectors', {})
        registry = Registry(**config.get('tags', {}))

        components = dict(platform=Platform(), registry=registry)
        injector = Injector(components)

        instance_config = config.get('instance_config', {})

        known = self._load_collectors()

        collectors = self._build_collectors(
            known, configured, injector, instance_config)

        return collectors, registry

    def _build_collectors(self, known, configured, injector,
                          instance_config):
        collectors = []

        for c in configured:
            c = dict(c)

            type = c.pop('type', None)

            if type is None:
                raise Exception(
                    "'type' is required in collector configuration")

            path = known.get(type, None)

            if path is None:
                raise Exception(
                    "'{0}' is not a known collector type".format(type))

            child = injector.child(dict(config=c))
            collector = Collector(
                path, type, self._out, child, instance_config)
            collectors.append(collector)

        return collectors

    def _load_collectors(self):
        collectors = dict()

        for p in self._ns.collectors:
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
