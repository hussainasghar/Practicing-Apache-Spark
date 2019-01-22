package spamfiltering.rdds

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 1/5/18.
  */
object Test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("Test")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/SMSSpamCollection")


    val stopWords= Seq("the", "a", "", "in", "on", "at", "as", "not", "for","to","will","its",
      "to","was","were","he","has","with","as","from","by")
    // Ignore all useless characters
    val ignoredChars = Seq(",", ":", ";", "/", "<", ">", "." , "*", "&","(" ,")","#", "+", "'" ,
      "/","(", ")", "?", "-", "\",","!")

    // Invoke RDD API and transform input data
    val textsRDD = data.map( r => {
      // Get rid of all useless characters
      var emailText = r.toLowerCase
      for( c <- ignoredChars) {
        emailText = emailText.replace(c, " ")
      }
      // Remove empty and uninteresting words
      val words = emailText.filter(w => !stopWords.contains(w)).distinct
      words


    })
    textsRDD


   val lowercaseData = data.map(_.toLowerCase)

    val spam = textsRDD.filter(_.startsWith("spam")).map(_.split(" ").toSeq)
    val ham = lowercaseData.filter(_.startsWith("ham")).map(_.split(" ").toSeq)

    val tf = new HashingTF()

    val spams:RDD[Vector] = tf.transform(spam)
    val hams: RDD[Vector] = tf.transform(ham)

    val positiveExamples = spams.map(features => LabeledPoint(1.0, features))
    val negativeExamples = hams.map(features => LabeledPoint(0.0, features))


    val trainingData = positiveExamples.union(negativeExamples)

    // Split data into training (60%) and test (40%).
    val splits = trainingData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val accuracy1 = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / test.count()

    println(accuracy1)

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
