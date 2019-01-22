//package spark_mllib.spamfiltering.dfds
//
//import scala.beans.BeanInfo
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
//import org.apache.spark.sql.{Row, SQLContext, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, Tokenizer}
//import org.apache.spark.ml.linalg.Vector
//import org.apache.spark.sql.{Row, SQLContext, SparkSession}
//
//
///**
//  * Created by hussain on 1/7/18.
//  */
//
//@BeanInfo
//case class LabeledDocument(id: Long, text: String, label: Double)
//
//
//@BeanInfo
//case class Document(id: Long, text: String)
//
//
//object ExampleClassification {
//
//  def main(args: Array[String]): Unit = {
//
//
//  val spark = SparkSession.builder().master("local[*]").appName("SparkSessionZipsExample").getOrCreate()
//
//  // Prepare training documents, which are labeled.
//  val training = spark.createDataFrame(Seq(
//    LabeledDocument(0L, "a b c d e spark", 1.0),
//    LabeledDocument(1L, "b d", 0.0),
//    LabeledDocument(2L, "spark f g h", 1.0),
//    LabeledDocument(3L, "hadoop mapreduce", 0.0)))
//
//  // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
//  val tokenizer = new Tokenizer()
//    .setInputCol("text")
//    .setOutputCol("words")
//
//    tokenizer.transform(training).show()
//
//  val hashingTF = new HashingTF()
//    .setNumFeatures(1000)
//    .setInputCol(tokenizer.getOutputCol)
//    .setOutputCol("features")
//
//
//  val lr = new LogisticRegression()
//    .setMaxIter(10)
//    .setRegParam(0.001)
//
//  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
//
//  // Fit the pipeline to training documents.
//  val model = pipeline.fit(training.toDF())
//
//
//  // Prepare test documents, which are unlabeled.
//  val test = spark.createDataFrame(Seq(
//    Document(4L, "spark i j k"),
//    Document(5L, "l m n"),
//    Document(6L, "spark hadoop spark"),
//    Document(7L, "apache hadoop")))
//
//  // Make predictions on test documents.
//  model.transform(test.toDF()).show()
//   /* .select("id", "text", "probability", "prediction")
//    .collect()
//    .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//      println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//    }
//*/
//  spark.stop()
//
//  }
//
//}
