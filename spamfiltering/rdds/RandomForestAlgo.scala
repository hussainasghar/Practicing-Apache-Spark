package spamfiltering.rdds

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by hussain on 1/3/18.
  */
object RandomForestAlgo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spam-Classifier").setMaster("local[3]");
    val sc = new SparkContext(conf);

    // Load and parse the data file.
    // val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

   // val spam = sc.textFile("src/main/resources/spam.txt")
  //  val normal = sc.textFile("src/main/resources/ham.txt")

  val rdd = sc.textFile("src/main/resources/SMSSpamCollection")

    val lowercaseData = rdd.map(_.toLowerCase)

    val spam = rdd.filter(_.startsWith("spam"))
    val normal = rdd.filter(_.startsWith("ham"))

    val stopWords= Seq("the", "a", "", "in", "on", "at", "as", "not", "for","to","will","its",
      "to","was","were","he","has","with","as","from","by")
    // Ignore all useless characters
    val ignoredChars = Seq(",", ":", ";", "/", "<", ">", "." , "*", "&","(" ,")","#", "+", "'" ,
      "/","(", ")", "?", "-", "\",","!")

    // Invoke RDD API and transform input data
    val spamRDD = spam.map( r => {
      // Get rid of all useless characters
      var emailText = r.toLowerCase
      for( c <- ignoredChars) {
        emailText = emailText.replace(c, " ")
      }
      // Remove empty and uninteresting words
      val words = emailText.split(" ").filter(w => !stopWords.contains(w)).distinct
      words.toSeq
    })
    spamRDD.foreach(println)


    val hamRDD = normal.map( r => {
      // Get rid of all useless characters
      var emailText = r.toLowerCase
      for( c <- ignoredChars) {
        emailText = emailText.replace(c, " ")
      }
      // Remove empty and uninteresting words
      val words = emailText.split(" ").filter(w => !stopWords.contains(w)).distinct
      words.toSeq
    })
    hamRDD.foreach(println)
  //  val spam = sc.textFile("src/main/resources/spam.txt")
  //  val normal = sc.textFile("src/main/resources/ham.txt")



    val tf = new HashingTF(numFeatures = 10000)


    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))


    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))


    val data=positiveExamples.union(negativeExamples)


    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 15
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 6
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    val spamTest = tf.transform("Congrats!,You have won a lottery ....".split(" "))
    val nonSpamTest = tf.transform("I'm Doing Scala coding on Spark.It is a good platform for Big Data processing ....".split(" "))

    println(model.predict(spamTest))
    println(model.predict(nonSpamTest))

    // Test on a positive example (spam) and a negative one (normal).
    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))

    println(model.predict(posTest))
    println(model.predict(negTest))


    /* // Save and load model
     model.save(sc, "target/tmp/myRandomForestClassificationModel")
     val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")*/


  }
}
