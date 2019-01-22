//package spark_mllib.spamfiltering.dfds
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
//// $example off$
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by hussain on 1/7/18.
//  */
//object TfidExample {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().master("local[*]").appName("Tokenizer").getOrCreate()
//
//    val sentenceData = spark.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (0, "I wish Java could use case classes"),
//      (1, "Logistic regression models are neat")
//    )).toDF("label", "sentence")
//
//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//    val wordsData = tokenizer.transform(sentenceData)
//    val hashingTF = new HashingTF()
//      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)
//    val featurizedData = hashingTF.transform(wordsData)
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData).show()
// //   rescaledData.select("features", "label").take(3).foreach(println)
//  }
//}
