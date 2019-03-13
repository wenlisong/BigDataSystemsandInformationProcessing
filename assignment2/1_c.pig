lines = LOAD 'output-1-b' as (bigram:chararray,year:int,match_count:int,volume_count:int);
groups = GROUP lines BY bigram;
avgs = FOREACH groups GENERATE group as bigram, AVG(lines.match_count) as avg_count;
STORE avgs INTO 'hdfs://localhost:9000/user/songwenli/output-1-c' USING PigStorage('\t');