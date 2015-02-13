import multiprocessing as mp
import logging
import yaml
import os.path
import sys
import time
import queue

from semcollect.registry import Registry
from semcollect.scope import Scope
from semcollect.platform import Platform
from semcollect.collector import Collector

log = logging.getLogger(__name__)

TASK_MOD = 2 ** 20


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
                if v is None:
                    log.debug("%s: None", tags)
                else:
                    log.debug("%s: %0.2f", tags, v)

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

        components = dict(platform=Platform())
        collectors = config.get('collectors', {})

        registry = Registry(**config.get('tags', {}))
        scope = Scope(collectors, registry, components)

        blacklist = set(config.get('blacklist', []))
        instance_config = config.get('instance_config', {})

        collectors = self._load_collectors(registry, scope, blacklist,
                                           instance_config)
        return collectors, registry

    def _load_collectors(self, registry, scope, blacklist, instance_config):
        collectors = []

        for p in self._ns.collectors:
            if not os.path.isdir(p):
                continue

            for n in os.listdir(p):
                if n.startswith('.') or not n.endswith('.py'):
                    continue

                path = os.path.join(p, n)
                name = os.path.basename(path)

                s = scope.collector(name)

                try:
                    c = Collector(path, self._out, s, instance_config)
                except:
                    s.free()
                    raise

                collectors.append(c)

        return collectors


def load_config(path):
    if path is None or not os.path.isfile(path):
        return {}

    with open(path) as f:
        return yaml.load(f)
