package spamfiltering.rdds

import org.apache.spark.{SparkConf, SparkContext, mllib}
import org.apache.spark.mllib
import org.apache.spark.mllib.feature.{IDFModel, IDF, HashingTF}
import org.apache.spark.rdd.RDD


/**
  * Created by hussain on 1/5/18.
  */
object TextFiltering_Methods {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spam-Classifier").setMaster("local[3]");
    val sc = new SparkContext(conf);


    def load(dataFile: String): RDD[Array[String]] = {
      // Load file into memory, split on space and filter all empty lines
      sc.textFile(dataFile).map(l => l.split(" ")).filter(r => !r(0).isEmpty)
    }

    // Tokenizer
    // For each sentence in input RDD it provides array of string representing individual interesting words in the sentence
    def tokenize(dataRDD: RDD[String]): RDD[Seq[String]] = {
      // Ignore all useless words
      val stopWords= Seq("the", "a", "", "in", "on", "at", "as", "not", "for","to","will","its",
        "to","was","were","he","has","with","as","from","by")
      // Ignore all useless characters
      val ignoredChars = Seq(",", ":", ";", "/", "<", ">", "." , "(", ")", "?", "-", "\",","!")

      // Invoke RDD API and transform input data
      val textsRDD = dataRDD.map( r => {
        // Get rid of all useless characters
        var emailText = r.toLowerCase
        for( c <- ignoredChars) {
          emailText = emailText.replace(c, " ")
        }
        // Remove empty and uninteresting words
        val words = emailText.split(" ").filter(w => !stopWords.contains(w) && w.length>2).distinct
        words.toSeq
      })
      textsRDD
    }


    def tokenize1(content: String) : Seq[String] = {
      // split by word, but include internal punctuation
      // so we can have domain "enron.com", etc.
      // but strip ending punctuation
      val pure_numbers="""[^0-9]*""".r
      return content.split("""\W+""")
        .map(_.toLowerCase)
        .filter(token => pure_numbers.pattern.matcher(token).matches)
        .toSeq
    }

    val lines = sc.textFile("/wikipedia")

    val words = lines.flatMap(_.split(" ")).filter(_ != "")


  }
}
