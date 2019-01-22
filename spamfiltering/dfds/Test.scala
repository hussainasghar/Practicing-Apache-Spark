/*
package spark_mllib.spamfiltering.dfds

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * Created by hussain on 1/6/18.
  */
object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering")
     // .config("spark.sql.warehouse.dir", warehouseLocation)
     // .enableHiveSupport()
      .getOrCreate()

    val sentenceDataFrame = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes I saw the red baloon" ),
      (2, "Logistic,regression,models,are,neat,Mary,had,a,little,lamb"),
      (3, " a the on in or for of to my you"),
      (4, ". ; * & $ # - ( ) < > "),
      (5, " A1 A2 Lol 4")
    )).toDF("label", "sentence")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    tokenizer.transform(sentenceDataFrame).show()

    val regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("regexFreeWords").setPattern("\\W")
    regexTokenizer.transform(sentenceDataFrame).show()
    // alternatively .setPattern("\\w+").setGaps(false)

//    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
//    regexTokenized.select("words", "label").take(3).foreach(println)
//
//

  /*  val remover = new StopWordsRemover().setInputCol("sentence").setOutputCol("filtered")
    remover.transform(sentenceDataFrame).show()*/


   /* val removed = remover.transform(sentenceDataFrame)
    removed.select("words", "label").take(3).foreach(println)
*/

 /*   val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words", "label").take(3).foreach(println)

    tokenized.show()
*/

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(sentenceDataFrame)

  /*  // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
*/

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

  }
}
*/
