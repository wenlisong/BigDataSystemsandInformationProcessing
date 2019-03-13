lines = LOAD 'output-1-c' as (bigram:chararray,avg_count:int);
sorted = ORDER lines BY avg_count DESC;
top_20 = LIMIT sorted 20;
STORE top_20 INTO 'hdfs://localhost:9000/user/songwenli/output-1-d' USING PigStorage('\t');