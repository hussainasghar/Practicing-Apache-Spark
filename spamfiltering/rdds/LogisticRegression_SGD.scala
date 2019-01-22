package spamfiltering.rdds

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.tree.DecisionTree
//import org.apache.spark.mllib.RandomForest.trainClassifier
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hussain on 12/18/17.
  */
object LogisticRegression_SGD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spam-Classifier").setMaster("local[3]");
    val sc = new SparkContext(conf);

    // val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/spambase/spambase.data"

   /* val data = sc.textFile("src/main/resources/SMSSpamCollection")

    val spam = data.filter(_.startsWith("spam"))
    val normal = data.filter(_.startsWith("ham"))*/

        val spam = sc.textFile("src/main/resources/spam.txt")
        val normal = sc.textFile("src/main/resources/ham.txt")


    // Create a HashingTF instance to map email text to vectors of 10,000 features.
    val tf = new HashingTF(numFeatures = 10000)

    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1.0, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0.0, features))


    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.


    val Array(training, test) = trainingData.randomSplit(Array(0.6, 0.4))

    // Run Logistic Regression using the SGD algorithm.
    val model = new LogisticRegressionWithSGD().run(training)

    //Function
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
    //Accuracy upto 91.44%

    // Get evaluation metrics.
    val metrics1 = new BinaryClassificationMetrics(predictionAndLabels)

    val auROC = metrics1.areaUnderROC()

    println("Area under ROC = " + auROC)

    val spamTest = tf.transform("Congrats!,You have won a lottery ....".split(" "))
    val nonSpamTest = tf.transform("I'm Doing Scala coding on Spark.It is a good platform for Big Data processing ....".split(" "))

    println(model.predict(spamTest))
    println(model.predict(nonSpamTest))

    // Test on a positive example (spam) and a negative one (normal).
    val posTest = tf.transform(
      "OMG GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))

    println(model.predict(posTest))
    println(model.predict(negTest))

    /*  // Save and load model
        model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
        val sameModel = LogisticRegressionModel.load(sc,
          "target/tmp/scalaLogisticRegressionWithLBFGSModel")*/

  }
}
