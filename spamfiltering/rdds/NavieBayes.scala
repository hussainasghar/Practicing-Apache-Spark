package spamfiltering.rdds

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by hussain on 1/5/18.
  */
object NavieBayes {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SVM").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
   // val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    val spam = sc.textFile("src/main/resources/spam.txt")
    val normal = sc.textFile("src/main/resources/ham.txt")

    // Create a HashingTF instance to map email text to vectors of 10,000 features.
    val tf = new HashingTF(numFeatures = 10000)

    // Each email is split into words, and each word is mapped to one feature.
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))

    val data = positiveExamples.union(negativeExamples)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0)

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy1 = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println(accuracy1)

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    val posTest = tf.transform(
      "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))

    println("Prediction for positive test example: " + model.predict(posTest))
    println("Prediction for negative test example: " + model.predict(negTest))

   /* // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")*/
  }
}
