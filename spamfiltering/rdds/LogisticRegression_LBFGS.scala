package spamfiltering.rdds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by hussain on 1/5/18.
  */
object LogisticRegression_LBFGS {
  def main(args: Array[String]): Unit = {

  val conf = new SparkConf().setAppName("Spam-Classifer").setMaster("local[3]");
  val sc = new SparkContext(conf);


    // Load training data in LIBSVM format.
   // val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/spambase/spambase.data"
   //  val spam = sc.textFile("src/main/resources/spam.txt")
   //  val ham = sc.textFile("src/main/resources/ham.txt")

   val data = sc.textFile("src/main/resources/SMSSpamCollection")

   val lowercoseData = data.map(_.toLowerCase)

    val spam = lowercoseData.filter(_.startsWith("spam"))
    val ham = lowercoseData.filter(_.startsWith("ham"))

    val tf = new HashingTF(numFeatures = 1000)

    val spamFeatures = spam.map(emailText=>tf.transform(emailText.split(" ")))
    val hamFeatures = ham.map(emailText=>tf.transform(emailText.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))


    val trainingData = positiveExamples.union(negativeExamples)

    //tuple
    // Split data into training (60%) and test (40%).
    val splits = trainingData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS().run(training)

    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }


    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Get evaluation metrics.
    val metrics1 = new BinaryClassificationMetrics(predictionAndLabels)
    val auPR = metrics1.areaUnderPR()
    val auROC = metrics1.areaUnderROC()

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
    // Accuracy Upto 91.93%

    /*  // Save and load model
    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/scalaLogisticRegressionWithLBFGSModel")*/

  }
}
