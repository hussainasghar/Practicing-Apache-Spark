package spark_mllib.spamfiltering.dfds

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.SparkSession


  // Created by hussain on 1/7/18.

object StopWordsRemoverr {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("StopWordsRemover").getOrCreate()


    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    remover.transform(dataSet).show()

    val removed = remover.transform(dataSet)

   removed.select("raw", "filtered").take(3).foreach(println)



  }
}

