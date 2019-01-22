//package spark_mllib.spamfiltering.ds
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
//// Created by hussain on 1/7/18.
//
//class WordCount {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering")
//      // .config("spark.sql.warehouse.dir", warehouseLocation)
//      // .enableHiveSupport()
//      .getOrCreate()
//
//    val ds = spark.read.text("/home/spark/1.6/lines").as[String]
//
//    val result = ds.flatMap(_.split(" "))
//      .filter(_ != "").toDF()
//      .groupBy("$value")
//     .agg(count("*") as "numOccurances")
//     // .orderBy("$numOccurances" desc)
//
//
//
//    ​
//  }
//}
//
