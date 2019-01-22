package spark_mllib.dfds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession


import scala.beans.BeanInfo

/**
  * Created by hussain on 1/9/18.
  */
object Mixing {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.
      builder
      .master("local[*]")
      .appName("Spam-Filtering")
      .getOrCreate()


 val data = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .option("delimeter", "\t").option("inferSchema", "true").load("src/main/resources/sms_spam.csv").cache()




  }
}
