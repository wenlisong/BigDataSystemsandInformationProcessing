A = LOAD 'a.txt' as (bigram:chararray,year:int,match_count:int,volume_count:int);
B = LOAD 'b.txt' as (bigram:chararray,year:int,match_count:int,volume_count:int);
merged = UNION A, B;
STORE merged INTO 'hdfs://localhost:9000/user/songwenli/output-1-b' USING PigStorage('\t');