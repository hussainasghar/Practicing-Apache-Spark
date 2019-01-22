//package spark_mllib.spamfiltering.dfds
//
//import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Tokenizer}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//
///**
//  * Created by hussain on 1/7/18.
//  */
//object Tokeniizer {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().master("local[*]").appName("Tokenizer").getOrCreate()
//
//    val sentenceDataFrame = spark.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (1, "I wish Java could use case classes I saw the red baloon" ),
//      (2, "Logistic,regression,models,are,neat,Mary,had,a,little,lamb"),
//      (3, " a the on in or for of to my you"),
//      (4, ". ; * & $ # - ( ) < > "),
//      (5, " A1 A2 Lol 4")
//    )).toDF("label", "sentence")
//
//  /*  val remover = new StopWordsRemover().setInputCol("sentence").setOutputCol("filtered")
//
//    println("StopWords Remover")
//    remover.transform(sentenceDataFrame).show()*/
//
//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//
//    println("Tokenizer")
//    tokenizer.transform(sentenceDataFrame).show()
//
//    val regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
//
//    println("Regex-Tokenizer")
//    regexTokenizer.transform(sentenceDataFrame).show()
//
//    val countTokens = udf { (words: Seq[String]) => words.length }
//
//    val tokenized = tokenizer.transform(sentenceDataFrame)
//    tokenized.select("sentence", "words")
//      .withColumn("tokens", countTokens(col("words"))).show(false)
//
//    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
//    regexTokenized.select("sentence", "words")
//      .withColumn("tokens", countTokens(col("words"))).show(false)
//    // $example off$
//
//    spark.stop()
//
//  }
//}
