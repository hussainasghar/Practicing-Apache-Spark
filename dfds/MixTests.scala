package spark_mllib.dfds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by hussain on 1/8/18.
  */
object MixTests {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spam-Classifier").setMaster("local[*]")//.set("spark.driver.host", "localhost");
    val sc = new SparkContext(conf);

    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering")
      .config("spark.master", "local").getOrCreate()

       val spam = spark.read.textFile("src/main/resources/spam.txt")
      val ham  = spark.read.textFile("src/main/resources/ham.txt")





  }
}
