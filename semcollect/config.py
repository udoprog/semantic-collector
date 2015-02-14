"""
Contains all the types and helpers necessary to validate a configuration.
"""

class ConfigException(Exception):
    pass


def path(p):
    if len(p) == 0:
        return ''

    p = list(p)
    parts = list()

    for i, v in enumerate(p):
        if type(v) == int:
            parts += ['[{0}]'.format(v)]
            continue

        if type(v) == str:
            if i == 0:
                parts += [v]
            else:
                parts += ['.{0}'.format(v)]

            continue

        raise Exception('invalid path: {0}', p)

    return ''.join(parts)


def dict_get(d, k):
    return d.get(k)


def dict_pop(d, k):
    return d.pop(k)


def as_string(key, **kw):
    kw['typename'] = 'string'
    return as_type(str, key, **kw)


def as_int(key, **kw):
    kw['typename'] = 'number'
    return as_type(int, key, **kw)


def as_float(key, **kw):
    kw['typename'] = 'number'
    return as_type(float, key, **kw)


def as_type(tp, key, typename=None, default=None, allow_none=False,
            access=dict_get, convert=True):
    if typename is None:
        typename = tp.__name__

    def _p(data, p):
        p = p + [key]

        try:
            v = access(data, key)
        except KeyError:
            v = default

        if v is None:
            if not allow_none:
                raise ConfigException(
                    '{0}: expected {1} value, but got none'.format(
                        path(p), typename))

            return None

        if isinstance(v, tp):
            return v

        # try to convert the given value
        if convert:
            try:
                return tp(v)
            except ValueError:
                pass

        raise ConfigException(
            '{0}: expected valid {1}, but got {2}'.format(
            path(p), typename, repr(v)))

    return _p


def as_dict(key):
    def _p(data, p):
        p = p + [key]

        v = data.get(key, {})

        if not isinstance(v, dict):
            raise ConfigException('{0}: expected dict'.format(path(p)))

        return v

    return _p


def as_list(key, default=None, allow_none=False, sub=None):
    core = as_type(list, key, default=default, allow_none=False,
                   convert=False)

    def _p(data, p):
        l = core(data, p)

        if l is None:
            return None

        if sub is None:
            return l

        p = p + [key]

        n = list()

        for i, v in enumerate(l):
            n.append(sub(v, p + [i]))

        return n

    return _p


def load_entry(f):
    """
    Used to decorate classes #load method to verify that they receive a dict.
    """
    def _p(cls, d, p=[]):
        if not isinstance(d, dict):
            raise ConfigException('{0}: expected dict'.format(path(p)))

        return f(cls, d, p=p)

    return _p


def as_load(key, t):
    def _p(data, p):
        p = p + [key]

        v = data.get(key, {})

        if not isinstance(v, dict):
            raise ConfigException('{0}: expected dict'.format(path(p)))

        return t.load(v, p)

    return _p


class InstanceConfig(object):
    # number of collections until the process will be recycled
    max_runs = as_int('max_runs', default=10000)
    # number of errors allowed until the process will be recycled
    max_errors = as_int('max_errors', default=5)
    # allowed timeout for a graceful shutdown to occur.
    graceful_timeout = as_float('graceful_timeout', default=2.0)
    # allowed timeout for a forceful shutdown to occur, failure to perform a
    # forceful shutdown will crash the collector.
    forceful_timeout = as_float('forceful_timeout', default=2.0)
    # maximum number of forceful attempts allowed.
    max_forceful_attempts = as_int('max_forceful_attempts', default=5)

    def __init__(self, max_runs, max_errors, graceful_timeout,
                 forceful_timeout, max_forceful_attempts):
        self.max_runs = max_runs
        self.max_errors = max_errors
        self.graceful_timeout = graceful_timeout
        self.forceful_timeout = forceful_timeout
        self.max_forceful_attempts = max_forceful_attempts

    @classmethod
    @load_entry
    def load(cls, data, p=[]):
        max_runs = cls.max_runs(data, p)
        max_errors = cls.max_errors(data, p)
        graceful_timeout = cls.graceful_timeout(data, p)
        forceful_timeout = cls.forceful_timeout(data, p)
        max_forceful_attempts = cls.max_forceful_attempts(data, p)
        return InstanceConfig(
            max_runs, max_errors, graceful_timeout, forceful_timeout,
            max_forceful_attempts)


class CollectorConfig(object):
    type = as_string('type', access=dict_pop)

    def __init__(self, type, config):
        self.type = type
        self.config = config

    @classmethod
    @load_entry
    def load(cls, data, p=[]):
        data = dict(data)
        type = cls.type(data, p)
        return CollectorConfig(type, data)

    def __repr__(self):
        return "<collector type={0} config={1}>".format(self.type, self.config)


class Root(object):
    collectors = as_list('collectors', sub=CollectorConfig.load)
    tags = as_dict('tags')
    instance_config = as_load('instance_config', InstanceConfig)

    def __init__(self, tags, collectors, instance_config):
        self.tags = tags
        self.collectors = collectors
        self.instance_config = instance_config

    @classmethod
    @load_entry
    def load(cls, data, p=[]):
        tags = cls.tags(data, p)
        collectors = cls.collectors(data, p)
        instance_config = cls.instance_config(data, p)
        return Root(tags, collectors, instance_config)
