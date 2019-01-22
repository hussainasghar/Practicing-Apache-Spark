package spark_mllib.dfds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LinearSVC, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.beans.BeanInfo

/**
  * Created by hussain on 1/8/18.
  */
object TEST_TryAgain {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spam-Classifier").setMaster("local[3]");
    val sc = new SparkContext(conf);

    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering").getOrCreate()

   val spam = sc.textFile("src/main/resources/spam.txt")
 //   val ham = sc.textFile("src/main/resources/ham.txt")


 //   val spam = spark.read.textFile("src/main/resources/spam.txt").filter(!_.isEmpty)

    val data = spark.createDataFrame(Seq(

      LabelFile(2L,"spam", 1.0),

      LabelFile(3L,"ham", 0.0)))

    data.show()

    val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("Regex-Tokenized").setPattern("\\W")
    // alternatively .setPattern("\\w+").setGaps(false)

    regexTokenizer.transform(data).show()

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("features")


    val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1)

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, hashingTF, lsvc))

    val model = pipeline.fit(data.toDF())

    //   Prepare test documents, which are unlabeled.

    val test = spark.createDataFrame(Seq(
      TestFile(4L,  "OMG GET cheap stuff by sending money to"),
      TestFile(5L, "Hi Dad I started studying Spark the other"),
      TestFile(6L, "Congrats You have won a lottery"),
      TestFile(7L, "I am Doing Scala coding on Spark.It is a good platform for Big Data processing")))


    // Make predictions on test documents.
   model.transform(test.toDF()).show()

    spark.stop()

  }
  @BeanInfo
  case class LabelFile(id: Long, text: String, label: Double)


  @BeanInfo
  case class TestFile(id: Long, text: String)
}
