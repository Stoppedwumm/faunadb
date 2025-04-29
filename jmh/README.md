# Benchmarks

This module uses [sbt-jmh](https://github.com/ktoso/sbt-jmh) to benchmark our functions.

## Usage

- run all benchmarks

```
sbt:jmh> jmh:run 
```

- run by name

```
sbt:jmh> jmh:run .*MyBench.*
```