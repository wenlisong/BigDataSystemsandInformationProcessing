CREATE TABLE BIGRAM (
  bigram STRING,
  year INT,
  match_count INT,
  volume_count INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
DESCRIBE BIGRAM;
load data inpath 'a.txt' into table BIGRAM;
load data inpath 'b.txt' into table BIGRAM;
INSERT OVERWRITE DIRECTORY 'hdfs://localhost:9000/user/songwenli/output-2'
select bigram, SUM(match_count)/COUNT(match_count) avg 
from BIGRAM GROUP BY bigram SORT BY avg DESC LIMIT 20;