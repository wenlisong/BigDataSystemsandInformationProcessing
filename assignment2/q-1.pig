A = LOAD 'a.txt' as (bigram:chararray,year:int,match_count:int,volume_count:int);
B = LOAD 'b.txt' as (bigram:chararray,year:int,match_count:int,volume_count:int);
merged = UNION A, B;
groups = GROUP merged BY bigram;
avgs = FOREACH groups GENERATE group as bigram, AVG(merged.match_count) as avg_count;
sorted = ORDER avgs BY avg_count DESC;
top_20 = LIMIT sorted 20;
STORE top_20 INTO 'hdfs://localhost:9000/user/songwenli/output-1' USING PigStorage('\t');