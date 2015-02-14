# semantic-collector

An agent for building collectors of structured metrics.

## Usage

Start the process.

```
bin/semcollect -i 30 -t 10 -p ./my-collectors
```

This indicates that you wish to collect data with an interval of 30 seconds,
and each individual collection is allowed to take at most 10 seconds.

You are also asking semantic-collector to load additional collectors from the
```my-collectors``` directory.

For even more options, see ```--help```.

## Collectors

Collectors are the backbone of this project.
The have the following properties.

* ```setup(scope)``` will be called by the main process, everything else will
  be run in an isolated process and intepreter.
* All metrics are written to shared memory, and must be prepared in the
  ```setup``` phase.
  This is abstracted through the ```registry```.
* A collector can have multiple instances, and each instance can have a unique
  configuration.

The following is one of the simplest collectors with state that you could
write.

```python
class Collector(object):
    def __init__(self, registry):
        self.m = registry.meter(what='example-metric')
        self.i = 0

    def __call__(self):
        i = self.i
        self.i += 1
        self.m.update(0.1 * i)


def setup(scope):
    registry = scope.require('registry')
    return Collector(registry)
```

### Bundled Collectors

* [cpu](collectors/cpu.py)
* [disk](collectors/disk.py)
* [iostat](collectors/iostat.py)
* [loadavg](collectors/loadavg.py)
* [memory (TODO)](collectors/memory.py)
* [net (TODO)](collectors/net.py)

## Output

Note: *To be implemented*

Outputs decide where the collected metrics are sent.

Output plugins run on the main loop of the collector.
