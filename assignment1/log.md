### small
```
19/01/25 04:13:01 INFO mapreduce.Job: Job job_1548388928144_0003 completed successfully
19/01/25 04:13:01 INFO mapreduce.Job: Counters: 49
        File System Counters
                FILE: Number of bytes read=455368
                FILE: Number of bytes written=1516564
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=248554
                HDFS: Number of bytes written=12189633
                HDFS: Number of read operations=9
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=8739
                Total time spent by all reduces in occupied slots (ms)=4634
                Total time spent by all map tasks (ms)=8739
                Total time spent by all reduce tasks (ms)=4634
                Total vcore-milliseconds taken by all map tasks=8739
                Total vcore-milliseconds taken by all reduce tasks=4634
                Total megabyte-milliseconds taken by all map tasks=8948736
                Total megabyte-milliseconds taken by all reduce tasks=4745216
        Map-Reduce Framework
                Map input records=21952
                Map output records=21952
                Map output bytes=411458
                Map output materialized bytes=455374
                Input split bytes=232
                Combine input records=0
                Combine output records=0
                Reduce input groups=599
                Reduce shuffle bytes=455374
                Reduce input records=21952
                Reduce output records=684722
                Spilled Records=43904
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=435
                CPU time spent (ms)=3140
                Physical memory (bytes) snapshot=773738496
                Virtual memory (bytes) snapshot=5923758080
                Total committed heap usage (bytes)=504889344
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=248322
        File Output Format Counters
                Bytes Written=12189633
19/01/25 04:13:01 INFO streaming.StreamJob: Output directory: output-1
```
```
19/01/25 04:22:47 INFO mapreduce.Job: Job job_1548388928144_0005 completed successfully
19/01/25 04:22:47 INFO mapreduce.Job: Counters: 49
        File System Counters
                FILE: Number of bytes read=13559083
                FILE: Number of bytes written=27723967
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=12189877
                HDFS: Number of bytes written=5206926
                HDFS: Number of read operations=9
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=12141
                Total time spent by all reduces in occupied slots (ms)=5598
                Total time spent by all map tasks (ms)=12141
                Total time spent by all reduce tasks (ms)=5598
                Total vcore-milliseconds taken by all map tasks=12141
                Total vcore-milliseconds taken by all reduce tasks=5598
                Total megabyte-milliseconds taken by all map tasks=12432384
                Total megabyte-milliseconds taken by all reduce tasks=5732352
        Map-Reduce Framework
                Map input records=684722
                Map output records=684722
                Map output bytes=12189633
                Map output materialized bytes=13559089
                Input split bytes=212
                Combine input records=0
                Combine output records=0
                Reduce input groups=283903
                Reduce shuffle bytes=13559089
                Reduce input records=684722
                Reduce output records=283903
                Spilled Records=1369444
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=316
                CPU time spent (ms)=6590
                Physical memory (bytes) snapshot=773267456
                Virtual memory (bytes) snapshot=5920833536
                Total committed heap usage (bytes)=510132224
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=12189665
        File Output Format Counters
                Bytes Written=5206926
19/01/25 04:22:47 INFO streaming.StreamJob: Output directory: output-2
```
### median
```
19/01/25 06:49:23 INFO mapreduce.Job: Job job_1548388928144_0007 completed successfully
19/01/25 06:49:23 INFO mapreduce.Job: Counters: 51
        File System Counters
                FILE: Number of bytes read=4409720
                FILE: Number of bytes written=9425313
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=2514822
                HDFS: Number of bytes written=1212506960
                HDFS: Number of read operations=9
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Killed map tasks=1
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=1
                Rack-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=9379
                Total time spent by all reduces in occupied slots (ms)=95929
                Total time spent by all map tasks (ms)=9379
                Total time spent by all reduce tasks (ms)=95929
                Total vcore-milliseconds taken by all map tasks=9379
                Total vcore-milliseconds taken by all reduce tasks=95929
                Total megabyte-milliseconds taken by all map tasks=9604096
                Total megabyte-milliseconds taken by all reduce tasks=98231296
        Map-Reduce Framework
                Map input records=201672
                Map output records=201672
                Map output bytes=4006370
                Map output materialized bytes=4409726
                Input split bytes=236
                Combine input records=0
                Combine output records=0
                Reduce input groups=610
                Reduce shuffle bytes=4409726
                Reduce input records=201672
                Reduce output records=60894136
                Spilled Records=403344
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=619
                CPU time spent (ms)=152040
                Physical memory (bytes) snapshot=802054144
                Virtual memory (bytes) snapshot=5944754176
                Total committed heap usage (bytes)=480247808
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=2514586
        File Output Format Counters
                Bytes Written=1212506960
19/01/25 06:49:23 INFO streaming.StreamJob: Output directory: median-output/output-1
```
```
19/01/25 06:57:25 INFO mapreduce.Job: Job job_1548388928144_0008 completed successfully
19/01/25 06:57:25 INFO mapreduce.Job: Counters: 51
        File System Counters
                FILE: Number of bytes read=2668590680
                FILE: Number of bytes written=4004905551
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=1212540808
                HDFS: Number of bytes written=534936480
                HDFS: Number of read operations=30
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Killed map tasks=7
                Launched map tasks=16
                Launched reduce tasks=1
                Data-local map tasks=10
                Rack-local map tasks=6
                Total time spent by all maps in occupied slots (ms)=1205939
                Total time spent by all reduces in occupied slots (ms)=255438
                Total time spent by all map tasks (ms)=1205939
                Total time spent by all reduce tasks (ms)=255438
                Total vcore-milliseconds taken by all map tasks=1205939
                Total vcore-milliseconds taken by all reduce tasks=255438
                Total megabyte-milliseconds taken by all map tasks=1234881536
                Total megabyte-milliseconds taken by all reduce tasks=261568512
        Map-Reduce Framework
                Map input records=60894136
                Map output records=60894136
                Map output bytes=1212506960
                Map output materialized bytes=1334295286
                Input split bytes=1080
                Combine input records=0
                Combine output records=0
                Reduce input groups=26325068
                Reduce shuffle bytes=1334295286
                Reduce input records=60894136
                Reduce output records=26325068
                Spilled Records=182682408
                Shuffled Maps =9
                Failed Shuffles=0
                Merged Map outputs=9
                GC time elapsed (ms)=2835
                CPU time spent (ms)=487730
                Physical memory (bytes) snapshot=2805424128
                Virtual memory (bytes) snapshot=19756810240
                Total committed heap usage (bytes)=1914175488
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=1212539728
        File Output Format Counters
                Bytes Written=534936480
19/01/25 06:57:25 INFO streaming.StreamJob: Output directory: output-2
```
### large
```
19/01/25 07:29:47 INFO mapreduce.Job: Job job_1548388928144_0009 completed successfully
19/01/25 07:29:47 INFO mapreduce.Job: Counters: 49
        File System Counters
                FILE: Number of bytes read=47494780
                FILE: Number of bytes written=95595427
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=27582810
                HDFS: Number of bytes written=9779790846
                HDFS: Number of read operations=9
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=20283
                Total time spent by all reduces in occupied slots (ms)=756738
                Total time spent by all map tasks (ms)=20283
                Total time spent by all reduce tasks (ms)=756738
                Total vcore-milliseconds taken by all map tasks=20283
                Total vcore-milliseconds taken by all reduce tasks=756738
                Total megabyte-milliseconds taken by all map tasks=20769792
                Total megabyte-milliseconds taken by all reduce tasks=774899712
        Map-Reduce Framework
                Map input records=2097150
                Map output records=2097150
                Map output bytes=43300474
                Map output materialized bytes=47494786
                Input split bytes=232
                Combine input records=0
                Combine output records=0
                Reduce input groups=7120
                Reduce shuffle bytes=47494786
                Reduce input records=2097150
                Reduce output records=501521155
                Spilled Records=4194300
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=4273
                CPU time spent (ms)=1246670
                Physical memory (bytes) snapshot=875368448
                Virtual memory (bytes) snapshot=5944451072
                Total committed heap usage (bytes)=559939584
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=27582578
        File Output Format Counters
                Bytes Written=9779790846
19/01/25 07:29:47 INFO streaming.StreamJob: Output directory: large-output/output-1
```
```
19/01/25 08:04:18 INFO mapreduce.Job: Job job_1548388928144_0010 completed successfully
19/01/25 08:04:18 INFO mapreduce.Job: Counters: 51
        File System Counters
                FILE: Number of bytes read=31903742073
                FILE: Number of bytes written=42701521973
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=9780094445
                HDFS: Number of bytes written=1300169880
                HDFS: Number of read operations=222
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Killed map tasks=1
                Launched map tasks=74
                Launched reduce tasks=1
                Data-local map tasks=72
                Rack-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=9435874
                Total time spent by all reduces in occupied slots (ms)=1188066
                Total time spent by all map tasks (ms)=9435874
                Total time spent by all reduce tasks (ms)=1188066
                Total vcore-milliseconds taken by all map tasks=9435874
                Total vcore-milliseconds taken by all reduce tasks=1188066
                Total megabyte-milliseconds taken by all map tasks=9662334976
                Total megabyte-milliseconds taken by all reduce tasks=1216579584
        Map-Reduce Framework
                Map input records=501521155
                Map output records=501521155
                Map output bytes=9779790846
                Map output materialized bytes=10782833594
                Input split bytes=8687
                Combine input records=0
                Combine output records=0
                Reduce input groups=63906296
                Reduce shuffle bytes=10782833594
                Reduce input records=501521155
                Reduce output records=63906296
                Spilled Records=1985031622
                Shuffled Maps =73
                Failed Shuffles=0
                Merged Map outputs=73
                GC time elapsed (ms)=30603
                CPU time spent (ms)=3666140
                Physical memory (bytes) snapshot=21187596288
                Virtual memory (bytes) snapshot=145850306560
                Total committed heap usage (bytes)=15005646848
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=9780085758
        File Output Format Counters
                Bytes Written=1300169880
19/01/25 08:04:18 INFO streaming.StreamJob: Output directory: large-output/output-2
```
