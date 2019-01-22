//package spark_mllib.dfds
//
//import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import spamfiltering.dfds.LogisticRegressionDF.TestFile
//
//import scala.beans.BeanInfo
////import org.apache.spark.sql.UserDefinedFunction
//import java.util.regex.Pattern
//
//import org.apache.spark.sql.expressions.UserDefinedFunction
//
///**
//  * Created by hussain on 1/9/18.
//  */
//object Net {
//
//
//  def main(args: Array[String]): Unit = {
//
//  /*  val smsSpamCollectionDf: Dataframe = sqlContext
//      .read
//      .format("com.databricks.spark.csv")
//      .option("delimiter", "\t")
//      .option("header", "false")
//      .option("mode", "PERMISSIVE")
//      .schema(schemaSmsSpamCollection)
//      .load(smsSpamCollectionPath)
//      .cache()*/
//
//    val spark = SparkSession.builder().appName("Spam-Classifer").master("local[*]").getOrCreate()
//
//    val loadData = spark.read.format("com.databricks.spark.csv").option("header", "false")
//      .option("delimeter","\t").load("src/main/resources/sms_spam.csv").cache()
//
//    loadData.show()
//
//
//  /*  val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("Regex-Tokenized").setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
//
//    regexTokenizer.transform(loadData).show()*/
//
//   /* val bagOfWordsFrom = udf{
//      (smsText: String) => smsText.split(" ").toList.map(_.trim)
//    }*/
//
//
//   /* val normalizeCurrencySymbol = udf{
//      (text: String) =>
//        val regex = "[\\$\\€\\£]"
//        val pattern = Pattern.compile(regex)
//        val matcher = pattern.matcher(text)
//
//        matcher.replaceAll(" normalizedcurrencysymbol ")
//    }
//
//    // Using an obvious toLowerCase UDF to ignore case:
//    val smsSpamCollectionDfLowerCased = smsSpamCollectionDf
//      .select(label,toLowerCase(smsText)
//          .as(smsText.toString))
//
//    // Doing the actual normalization on the lower cased data:
//    val smsSpamCollectionDfNormalized = smsSpamCollectionDfLowerCased
//      .withColumn("NormalizedSmsText",
//        removePunctuationAndSpecialChar(
//          normalizeNumber(
//            removeHTMLCharacterEntities(
//              normalizeCurrencySymbol(
//                normalizeEmoticon(
//                  normalizeURL(
//                    normalizeEmailAddress(smsText)
//                  )))))))
//
//
//    val bagOfWordsFrom = udf{
//      (smsText: String) => smsText.split(" ").toList.map(_.trim)
//    }
//
//    val encodeLabel: UserDefinedFunction = udf {
//      (label: String) => if (label == "ham") 1.0 else 0.0
//    }
//
//    val tokenFrequencies = smsSpamCollectionBagOfWords
//      .select($"BagOfWords")
//      .flatMap(row => row.getAs[Seq[String]](0))
//      .toDF("Tokens")
//      .groupBy($"Tokens")
//      .agg(count("*").as("Frequency"))
//      .orderBy($"Frequency".desc)
//
//    val stopWords = sc.broadcast { Source.fromFile(stopWordsPath).getLines().to[Seq] }
//    val tokenFrequencies = smsSpamCollectionBagOfWords.select($"BagOfWords")
//      .flatMap(row => row.getAs[Seq[String]](0))
//      .filter(token => !(stopWords.value contains token))
//      .toDF("Tokens")
//      .groupBy($"Tokens")
//      .agg(count("*").as("Frequency"))
//      .orderBy($"Frequency".desc)
//
//
//
//    val doesLookSuspicious = udf {
//      (bagOfWords: Seq[String]) =>
//        val containsAlarmWords = Set("normalizednumber", "normalizedcurrencysymbol", "free") subsetOf bagOfWords.toSet
//
//        // Ham <--> positive class <--> 1.0; Spam <--> negative class <--> 0.0:
//        if (containsAlarmWords) 0.0 else 1.0
//    }
//
//
//    val predictionAndLabels = dataset
//      .withColumn("Prediction", doesLookSuspicious($"BagOfWords"))
//      .select($"LabelCode", $"Prediction")
//      .map(row => (row.getDouble(0), row.getDouble(1)))
//
//
//
//    val tn = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 0 }.count().toFloat
//    val fp = predictionAndLabels.filter { case (predicted, actual) => actual == 0 && predicted == 1 }.count().toFloat
//    val fn = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 0 }.count().toFloat
//    val tp = predictionAndLabels.filter { case (predicted, actual) => actual == 1 && predicted == 1 }.count().toFloat
//
//    printf(s"""|=================== Confusion matrix ==========================
//               |#############| %-15s                     %-15s
//               |-------------+-------------------------------------------------
//               |Predicted = 0| %-15f                     %-15f
//               |Predicted = 1| %-15f                     %-15f
//               |===============================================================
//         """.stripMargin, "Actual = 0", "Actual = 1", tn, fp, fn, tp)
//
//
//    import org.apache.spark.mllib.evaluation.MulticlassMetrics
//
//    val metrics = new MulticlassMetrics(predictionAndLabels)
//    val cfm = metrics.confusionMatrix
//
//    val tn = cfm(0, 0)
//    val fp = cfm(0, 1)
//    val fn = cfm(1, 0)
//    val tp = cfm(1, 1)
//*/
//
//
//
//
//
//
//
//  }
//}
