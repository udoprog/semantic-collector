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
