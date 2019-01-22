package spark_mllib.spamfiltering.rdds

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 1/16/18.
  */
object RF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Test")
    val sc = new SparkContext(conf)


    val data = sc.textFile("src/main/resources/SMSSpamCollection",4)


    val lowercaseData = data.map(_.toLowerCase)

    val spam = lowercaseData.filter(_.startsWith("spam"))
    val ham = lowercaseData.filter(_.startsWith("ham"))

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


    val hamRDD = ham.map( r => {
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

    val tf = new HashingTF()

    val spams:RDD[Vector] = tf.transform(spamRDD)
    val hams: RDD[Vector] = tf.transform(hamRDD)

    val positiveExamples = spams.map(features => LabeledPoint(1.0, features))
    val negativeExamples = hams.map(features => LabeledPoint(0.0, features))


    val trainingData = positiveExamples.union(negativeExamples)

    // Split data into training (70%) and test (30%).
    val splits = trainingData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2  // Problem specification parameters
    val categoricalFeaturesInfo = Map[Int, Int]() // Problem specification parameters
    val numTrees = 5 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4  //Stopping criteria
    val maxBins = 32 //Tunable parameters

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


    // Evaluate model on test instances and compute test error
    val labelAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count()
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
  }
}
