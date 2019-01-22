//
//package spark_mllib.spamfiltering.rdds
//
//import org.apache.spark.mllib.feature.HashingTF
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.tree.GradientBoostedTrees
//import org.apache.spark.mllib.tree.configuration.BoostingStrategy
//import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
//import org.apache.spark.mllib.util.MLUtils
//import org.apache.spark.mllib.tree.GradientBoostedTrees
//import org.apache.spark.mllib.tree.configuration.BoostingStrategy
//import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
//import org.apache.spark.mllib.util.MLUtils
//
//
///**
//  * Created by hussain on 1/5/18.
//  */
//object GradientBosstedTreesAlgo {
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("Gradient-Bossted-Tree").setMaster("local[3]")
//    val sc = new SparkContext(conf)
//
//    // Load and parse the data file.
//   // val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
//
//    val spam = sc.textFile("src/main/resources/spam.txt")
//    val normal = sc.textFile("src/main/resources/ham.txt")
//
//    // Create a HashingTF instance to map email text to vectors of 10,000 features.
//    val tf = new HashingTF(numFeatures = 10000)
//
//    // Each email is split into words, and each word is mapped to one feature.
//    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
//    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
//
//    // Create LabeledPoint datasets for positive (spam) and negative (normal) examples.
//    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
//    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
//
//
//    val trainingData = positiveExamples.union(negativeExamples)
//    trainingData.cache() // Cache since Logistic Regression is an iterative algorithm.
//
//
//    // Split the data into training and test sets (30% held out for testing)
//    val splits = trainingData.randomSplit(Array(0.7, 0.3))
//    val (training, testData) = (splits(0), splits(1))
//
//    // Train a GradientBoostedTrees model.
//    // The defaultParams for Classification use LogLoss by default.
//    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
//    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
//    boostingStrategy.treeStrategy.numClasses = 2
//    boostingStrategy.treeStrategy.maxDepth = 5
//    // Empty categoricalFeaturesInfo indicates all features are continuous.
//    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
//
//    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)
//
//    // Evaluate model on test instances and compute test error
//    val labelAndPreds = testData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
//    println(s"Test Error = $testErr")
//    println(s"Learned classification GBT model:\n ${model.toDebugString}")
//
//    val spamTest = tf.transform("Congrats!,You have won a lottery ....".split(" "))
//    val nonSpamTest = tf.transform("I'm Doing Scala coding on Spark.It is a good platform for Big Data processing ....".split(" "))
//
//    println(model.predict(spamTest))
//    println(model.predict(nonSpamTest))
//
//    // Test on a positive example (spam) and a negative one (normal).
//    val posTest = tf.transform(
//      "O M G GET cheap stuff by sending money to ...".split(" "))
//    val negTest = tf.transform(
//      "Hi Dad, I started studying Spark the other ...".split(" "))
//
//    println(model.predict(posTest))
//    println(model.predict(negTest))
//  /* // Save and load model
//    model.save(sc, "target/tmp/myGradientBoostingClassificationModel")
//    val sameModel = GradientBoostedTreesModel.load(sc,
//      "target/tmp/myGradientBoostingClassificationModel")*/
//
//
//
//  }
//}
//
