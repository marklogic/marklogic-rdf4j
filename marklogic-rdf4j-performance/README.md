#marklogic-sesame-performance 

This directory contains [jmh](http://openjdk.java.net/projects/code-tools/jmh/) performance benchmark suite for marklogic-sesame.

##Setup and Running

This perf suite utilises the same marklogic-sesame environment/marklogic setup (referenced in gradle.properties) and it will need to be operational to run these tests. 
  
To run performance test 

```
gradle jmh
```

##Example results

Perf test results are generated to [file://build/reports/jmh](file://build/reports/jmh).

human.txt
```
# VM invoker: /Library/Java/JavaVirtualMachines/jdk1.7.0_71.jdk/Contents/Home/jre/bin/java
# VM options: -Dfile.encoding=UTF-8 -Duser.country=US -Duser.language=en -Duser.variant
# Warmup: 1 iterations, 1 s each, 2 calls per op
# Measurement: 5 iterations, 1 s each
# Timeout: 10 min per iteration
# Threads: 1 thread, will synchronize iterations
# Benchmark mode: Throughput, ops/time
# Benchmark: com.marklogic.semantics.sesame.benchmarks.MarkLogicRepositoryConnectionNaivePerfTest.perfNaiveQuery1

# Run progress: 0.00% complete, ETA 00:01:00
# Fork: 1 of 10
# Warmup Iteration   1: 0.000 ops/s
Iteration   1: 8.673 ops/s
Iteration   2: 9.987 ops/s
Iteration   3: 11.124 ops/s
Iteration   4: 11.643 ops/s
Iteration   5: 11.130 ops/s

# Run progress: 10.00% complete, ETA 00:10:19
# Fork: 2 of 10
# Warmup Iteration   1: 2.533 ops/s
Iteration   1: 9.881 ops/s
Iteration   2: 11.056 ops/s
Iteration   3: 10.980 ops/s
Iteration   4: 11.769 ops/s
Iteration   5: 11.903 ops/s

# Run progress: 20.00% complete, ETA 00:09:06
# Fork: 3 of 10
# Warmup Iteration   1: 1.862 ops/s
Iteration   1: 8.821 ops/s
Iteration   2: 9.593 ops/s
Iteration   3: 9.490 ops/s
Iteration   4: 11.062 ops/s
Iteration   5: 12.383 ops/s

# Run progress: 30.00% complete, ETA 00:07:56
# Fork: 4 of 10
# Warmup Iteration   1: 1.779 ops/s
Iteration   1: 9.597 ops/s
Iteration   2: 11.070 ops/s
Iteration   3: 10.455 ops/s
Iteration   4: 11.539 ops/s
Iteration   5: 11.919 ops/s

# Run progress: 40.00% complete, ETA 00:06:47
# Fork: 5 of 10
# Warmup Iteration   1: 1.835 ops/s
Iteration   1: 9.847 ops/s
Iteration   2: 10.968 ops/s
Iteration   3: 10.755 ops/s
Iteration   4: 11.784 ops/s
Iteration   5: 11.804 ops/s

# Run progress: 50.00% complete, ETA 00:05:39
# Fork: 6 of 10
# Warmup Iteration   1: 2.580 ops/s
Iteration   1: 9.703 ops/s
Iteration   2: 10.725 ops/s
Iteration   3: 10.753 ops/s
Iteration   4: 11.708 ops/s
Iteration   5: 11.729 ops/s

# Run progress: 60.00% complete, ETA 00:04:31
# Fork: 7 of 10
# Warmup Iteration   1: 1.837 ops/s
Iteration   1: 9.788 ops/s
Iteration   2: 10.982 ops/s
Iteration   3: 10.560 ops/s
Iteration   4: 11.729 ops/s
Iteration   5: 12.231 ops/s

# Run progress: 70.00% complete, ETA 00:03:23
# Fork: 8 of 10
# Warmup Iteration   1: 1.742 ops/s
Iteration   1: 9.266 ops/s
Iteration   2: 10.918 ops/s
Iteration   3: 10.660 ops/s
Iteration   4: 11.404 ops/s
Iteration   5: 11.397 ops/s

# Run progress: 80.00% complete, ETA 00:02:15
# Fork: 9 of 10
# Warmup Iteration   1: 2.605 ops/s
Iteration   1: 10.107 ops/s
Iteration   2: 10.997 ops/s
Iteration   3: 11.047 ops/s
Iteration   4: 11.748 ops/s
Iteration   5: 11.972 ops/s

# Run progress: 90.00% complete, ETA 00:01:07
# Fork: 10 of 10
# Warmup Iteration   1: 2.614 ops/s
Iteration   1: 10.224 ops/s
Iteration   2: 10.925 ops/s
Iteration   3: 11.219 ops/s
Iteration   4: 11.653 ops/s
Iteration   5: 12.003 ops/s


Result: 10.894 ±(99.9%) 0.449 ops/s [Average]
  Statistics: (min, avg, max) = (8.673, 10.894, 12.383), stdev = 0.908
  Confidence interval (99.9%): [10.444, 11.343]


# Run complete. Total time: 00:11:17

Benchmark                                                                Mode  Samples   Score   Error  Units
c.m.s.s.b.MarkLogicRepositoryConnectionNaivePerfTest.perfNaiveQuery1    thrpt       50  10.894 ± 0.449  ops/s

Benchmark result is saved to build/reports/jmh/results.txt

```

results.txt - json file 

```
[
    {
        "benchmark" : "com.marklogic.semantics.sesame.benchmarks.MarkLogicRepositoryConnectionNaivePerfTest.perfNaiveQuery1",
        "mode" : "thrpt",
        "threads" : 1,
        "forks" : 10,
        "warmupIterations" : 1,
        "warmupTime" : "1 s",
        "measurementIterations" : 5,
        "measurementTime" : "1 s",
        "primaryMetric" : {
            "score" : 11.571959545993407,
            "scoreError" : 0.39984482861012854,
            "scoreConfidence" : [
                11.172114717383279,
                11.971804374603535
            ],
            "scorePercentiles" : {
                "0.0" : 9.778125444460247,
                "50.0" : 11.528308294213497,
                "90.0" : 12.554351286089638,
                "95.0" : 12.625001773315498,
                "99.0" : 12.82735407602003,
                "99.9" : 12.82735407602003,
                "99.99" : 12.82735407602003,
                "99.999" : 12.82735407602003,
                "99.9999" : 12.82735407602003,
                "100.0" : 12.82735407602003
            },
            "scoreUnit" : "ops/s",
            "rawData" : [
                [
                    10.246041612590336,
                    11.1661017764409,
                    11.510964636545719,
                    12.48451696957969,
                    12.574503935819731
                ],
                [
                    9.778125444460247,
                    11.62395607932903,
                    11.69968527846601,
                    12.45799874708127,
                    12.571416119740224
                ],
                [
                    10.384207005705257,
                    11.444326871103426,
                    11.58034263561472,
                    12.524389011101082,
                    12.82735407602003
                ],
                [
                    10.461208531115558,
                    11.326034720395052,
                    11.545651951881275,
                    12.469605336991084,
                    12.686721352476992
                ],
                [
                    10.519690230188354,
                    11.40127852183301,
                    11.558833126904428,
                    12.335421250230187,
                    12.489506584200239
                ],
                [
                    10.334635497406007,
                    11.33703792849643,
                    11.366100343431093,
                    11.299572528479269,
                    12.538724507205737
                ],
                [
                    10.51444553385783,
                    11.222869791401383,
                    11.598578549588831,
                    12.470138471980489,
                    12.035825494285563
                ],
                [
                    10.56409633399447,
                    11.109496910705282,
                    11.373787269932343,
                    12.352454344446425,
                    12.556087594854516
                ],
                [
                    10.155488999489688,
                    11.410945905972051,
                    11.294830703520251,
                    12.34520000987616,
                    12.500982220031574
                ],
                [
                    10.345799734630237,
                    11.097234825171602,
                    11.340984135707961,
                    11.688608282368005,
                    12.076169577023535
                ]
            ]
        },
        "secondaryMetrics" : {
        }
    }
]

```