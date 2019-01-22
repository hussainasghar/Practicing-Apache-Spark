package spark_mllib.dfds

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.beans.BeanInfo


/**
  * Created by hussain on 1/7/18.
  */

@BeanInfo
case class LabeledDocuments(id: Long, text: String, label: Double)


@BeanInfo
case class Documents(id: Long, text: String)


object LRDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering").getOrCreate()

    val training = spark.createDataFrame(Seq(
      LabeledDocuments(0L, "a b c d e spark", 1.0),
      LabeledDocuments(1L, "b d", 0.0),
      LabeledDocuments(2L, "spark f g h", 1.0),
      LabeledDocuments(3L, "hadoop mapreduce", 0.0)))

 //   val Array(trainingData, testData) = training.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")


    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training.toDF())


    // Prepare test documents, which are unlabeled.
    val test = spark.createDataFrame(Seq(
      Documents(4L, "spark i j k"),
      Documents(5L, "l m n"),
      Documents(6L, "spark hadoop spark"),
      Documents(7L, "apache hadoop")))

    // Make predictions on test documents.
    model.transform(test.toDF()).show()

    spark.stop()

  }
}
