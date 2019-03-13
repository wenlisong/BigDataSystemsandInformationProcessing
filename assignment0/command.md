```
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teragen 20000000 terasort/input-2G
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar terasort terasort/input-2G terasort/output-2G
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teravalidate terasort/output-2G terasort/check-2G
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teragen 200000000 terasort/input-20G
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar terasort terasort/input-20G terasort/output-20G
$ hadoop jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teravalidate terasort/output-20G terasort/check-20G

$ hdfs dfs -mkdir datasets
$ hdfs dfs -put ~/Downloads/ pyfiles
$ hdfs dfs -put ~/Downloads/Large_Dataset.txt datasets
$ hadoop jar ~/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.9.2.jar -D mapreduce.job.maps=16 -D mapreduce.job.reduces=8 -file ~/MapReduce_WordCount/mapper.py -mapper ~/MapReduce_WordCount/mapper.py -file ~/MapReduce_WordCount/reducer.py -reducer ~/MapReduce_WordCount/reducer.py -input datasets/Large_Dataset.txt -output output-wordcount

export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
$ hadoop com.sun.tools.javac.Main WordCount.java
$ jar cf wc.jar WordCount*.class
$ hadoop jar wc.jar WordCount datasets/Large_Dataset.txt output-java-wordcount
```