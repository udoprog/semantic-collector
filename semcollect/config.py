class InstanceConfig(object):
    def __init__(self, max_runs, max_errors, graceful_timeout,
                 forceful_timeout, max_forceful_attempts):
        self.max_runs = max_runs
        self.max_errors = max_errors
        self.graceful_timeout = graceful_timeout
        self.forceful_timeout = forceful_timeout
        self.max_forceful_attempts = max_forceful_attempts

    @classmethod
    def load(cls, inp, p=[]):
        max_runs = as_t(int, inp, 'max_runs', None, p)
        max_errors = as_t(int, inp, 'max_errors', None, p)
        graceful_timeout = as_t(float, inp, 'graceful_timeout', None, p)
        forceful_timeout = as_t(float, inp, 'forceful_timeout', None, p)
        max_forceful_attempts = as_t(
            int, inp, 'max_forceful_attempts', None, p)
        return InstanceConfig(
            max_runs, max_errors, graceful_timeout, forceful_timeout,
            max_forceful_attempts)


class Root(object):
    def __init__(self, tags, collectors, instance_config):
        self.tags = tags
        self.collectors = collectors
        self.instance_config = instance_config

    @classmethod
    def load(cls, inp, p=[]):
        tags = as_dict(inp, 'tags', p)
        collectors = as_list(inp, 'collectors', p)
        instance_config = as_type(InstanceConfig, inp, 'instance_config', p)
        return Root(tags, collectors, instance_config)


def path(p):
    return '.'.join(p)


def as_t(type, data, key, default, p):
    p = p + [key]

    v = data.get(key, default)

    if v is None:
        return None

    if not isinstance(v, type):
        try:
            return type(v)
        except ValueError:
            raise Exception('{0}: expected {1}'.format(path(p), type))

    return v


def as_dict(data, key, p):
    p = p + [key]

    v = data.get(key, {})

    if not isinstance(v, dict):
        raise Exception('{0}: expected dict'.format(path(p)))

    return v


def as_list(data, key, p):
    p = p + [key]

    v = data.get(key, [])

    if not isinstance(v, list):
        raise Exception('{0}: expected list'.format(path(p)))

    return v


def as_type(t, data, key, p):
    p = p + [key]

    v = data.get(key, {})

    if not isinstance(v, dict):
        raise Exception('{0}: expected dict'.format(path(p)))

    return t.load(v, p)
