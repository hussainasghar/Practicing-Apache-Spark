package spamfiltering.rdds

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by hussain on 1/3/18.
  */
object LSVMClassifier {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SVM").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
  //  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

  val data = sc.textFile("src/main/resources/SMSSpamCollection")

    val spam = data.filter(_.startsWith("spam"))
    val normal = data.filter(_.startsWith("ham"))

   /* val spam = sc.textFile("src/main/resources/spam.txt")
    val normal = sc.textFile("src/main/resources/ham.txt")*/

    // Create a HashingTF instance to map email text to vectors of 10,000 features.
    val tf = new HashingTF(numFeatures = 10000)

    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))


    val trainingData = positiveExamples.union(negativeExamples)
  //  trainingData.cache()

    // Split data into training (60%) and test (40%).
    val splits = trainingData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

/*
    val svmObj = new SVMWithSGD()
    svmObj.optimizer.setNumIterations(200)
      .setRegParam(0.1)
      .setUpdater(new L1Updater)

    val config_model = svmObj.run(training)*/


    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)


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

   /*
    // Save and load model
    model.save(sc, "target/tmp/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")*/
  }
}
