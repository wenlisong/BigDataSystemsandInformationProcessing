## question a
Download, unzip, copy to HDFS
```
$ wget --user bigdata --password spring2019bigdata \
http://mobitec.ie.cuhk.edu.hk/iems5730Spring2019/homework/hw1_small_dataset.zip
$ upzip hw1_small_dataset.zip
$ hdfs dfs -copyFromLocal hw1_small_dataset hw1_small_dataset
```

Run
```
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper1.py -mapper mapper1.py -file ~/reducer1.py -reducer reducer1.py \
-input hw1_small_dataset -output small-output/output-1

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input small-output/output-1 -output small-output/output-2
```

Check result
```
$ hdfs dfs -cat output-2/part* | awk '{if(($1-24)%100==0) print}' | \
sort -V > small-result.txt
```

## question b
Download, unzip, copy to HDFS
```
$ wget wget --user bigdata --password spring2019bigdata \
http://mobitec.ie.cuhk.edu.hk/iems5730Spring2019/homework/hw1_median_dataset.zip
$ wget wget --user bigdata --password spring2019bigdata \
http://mobitec.ie.cuhk.edu.hk/iems5730Spring2019/homework/hw1_large_dataset.zip

$ upzip hw1_median_dataset.zip
$ upzip hw1_large_dataset.zip

$ hdfs dfs -copyFromLocal hw1_median_dataset hw1_median_dataset
$ hdfs dfs -copyFromLocal hw1_large_dataset hw1_large_dataset
```
Run using median dataset
```
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper1.py -mapper mapper1.py -file ~/reducer1.py -reducer reducer1.py \
-input hw1_median_dataset -output median-output/output-1

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input median-output/output-1 -output median-output/output-2
```
Check result
```
$ hdfs dfs -cat median-output/output-2/part* | awk '{if(($1-524)%1000==0) print}' | \
sort -V > median-result.txt
```

Run using large dataset
```
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper1.py -mapper mapper1.py -file ~/reducer1.py -reducer reducer1.py \
-input hw1_large_dataset -output large-output/output-1

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output large-output/output-2
```
Check result
```
$ hdfs dfs -cat large-output/output-2/part* | awk '{if(($1-524)%1000==0) print}' | \
sort -V  > large-result.txt
```

### question c
Configaration
```
    <property>
            <name>mapreduce.map.memory.mb</name>
            <value>512</value>
    </property>
    <property>
            <name>mapreduce.reduce.memory.mb</name>
            <value>512</value>
    </property>
    <property>
            <name>mapreduce.input.fileinputformat.split.minsize</name>
            <value>134217728</value>
    </property>
    <property>
            <name>mapreduce.input.fileinputformat.split.minsize</name>
            <value>268435456</value>
    </property>
    <property>
            <name>mapreduce.input.fileinputformat.split.minsize</name>
            <value>536870912</value>
    </property>
```
Run with different #mappers and #reducers
```
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=18 -D mapreduce.job.reduces=16 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-18-16

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=18 -D mapreduce.job.reduces=32 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-18-32

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=18 -D mapreduce.job.reduces=64 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-18-64

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=36 -D mapreduce.job.reduces=16 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-36-16

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=36 -D mapreduce.job.reduces=32 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-36-32

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=36 -D mapreduce.job.reduces=64 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-36-64

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=72 -D mapreduce.job.reduces=16 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-72-16

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=72 -D mapreduce.job.reduces=32 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-72-32

$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.job.maps=72 -D mapreduce.job.reduces=64 \
-file ~/mapper2.py -mapper mapper2.py -file ~/reducer2.py -reducer reducer2.py \
-input large-output/output-1 -output questionC/output-72-64
```

Other
```
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar \
-D mapreduce.map.memory.mb=256 -D mapreduce.reduce.memory.mb=256 \
-D mapreduce.job.maps=128 -D mapreduce.job.reduces=64 \
-file ~/mapper1.py -mapper mapper1.py -file ~/reducer1.py -reducer reducer1.py \
-input hw1_small_dataset -output output-1

$ hdfs dfs -rm -r output
```