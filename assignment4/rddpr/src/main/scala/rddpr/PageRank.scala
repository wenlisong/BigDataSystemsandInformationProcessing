package rddpr

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author ${user.name}
 */
object PageRank {
  
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("pagerank").setMaster("yarn")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs:///user/ubuntu/web-Google.txt")
    val tmp_links = lines.map(line => {
                              val arr = line.split("\\s+")
                              (arr(0),arr(1))
                    })
    val links = tmp_links.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)
    for (i<- 1 to 30){
      val contribs = links.join(ranks).flatMap{
        case (start, (ends,rank)) =>
          ends.map(end => (end, rank/ends.size))
        }
      ranks = contribs.reduceByKey(_+_)
                      .mapValues(0.15+0.85*_)
    }
    ranks.sortBy(_._2, false).saveAsTextFile("hdfs:///user/ubuntu/pagerank-output-1")
  }
}
