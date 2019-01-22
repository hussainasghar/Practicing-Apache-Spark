package spark_mllib.spamfiltering.rdds

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 1/13/18.
  */
object Latest_Try {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Test")
    val sc = new SparkContext(conf)

    val spam = sc.textFile("src/main/resources/spam.txt")
    val ham = sc.textFile("src/main/resources/ham.txt")


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


    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

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
