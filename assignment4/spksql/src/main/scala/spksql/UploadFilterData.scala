package spksql

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object UploadFilterData {
  
  def main(args : Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("upload filter data")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.format("csv")
                  .option("header", "true")
                  .option("mode", "DROPMALFORMED")
                  .load("hdfs:///user/ubuntu/crime_incidents.csv")
    val mydf = df.select("CCN","REPORT_DAT","OFFENSE","METHOD","SHIFT","DISTRICT")
    mydf.show()
    mydf.groupBy("OFFENSE").count().show()
    mydf.groupBy("OFFENSE").count().show()
  }

}
