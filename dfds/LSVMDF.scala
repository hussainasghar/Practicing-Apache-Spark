package spark_mllib.dfds

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer}
import org.apache.spark.sql.SparkSession

import scala.beans.BeanInfo

/**
  * Created by hussain on 1/8/18.
  */
object LSVMDF {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("Spam-Filtering").getOrCreate()

    val data = spark.createDataFrame(Seq(

      LabelFile(2L,"Dear sir, I am a Prince in a far kingdom you have not heard of. " +
        " I want to send you money via wire transfer so please ...Get Viagra real cheap!  " +
        "Send money right away to ...Oh my gosh you can be really strong too with these drugs" +
        " found in the rainforest. Get them cheap right now ...YOUR COMPUTER HAS BEEN INFECTED! " +
        " YOU MUST RESET YOUR PASSWORD.  Reply to this email with your password and SSN " +
        "...THIS IS NOT A SCAM!  Send money and get access to awesome stuff really cheap " +
        "and never have to ..." , 1.0),

      LabelFile(3L,"Dear Spark Learner, Thanks so much for attending the Spark Summit 2014!  " +
        "Check out videos of talks from the summit at ...Hi Mom, Apologies for being late about " +
        "emailing and forgetting to send you the package.  I hope you and bro have been ...Wow, " +
        "hey Fred, just heard about the Spark petabyte sort.  I think we need to take time to try" +
        " it out immediately ...Hi Spark user list, This is my first question to this list," +
        " so thanks in advance for your help!  I tried running ...Thanks Tom for your email." +
        "  I need to refer you to Alice for this one.  I haven't yet figured out that part either" +
        " ...Good job yesterday!  I was attending your talk, and really enjoyed it." +
        "  I want to try out GraphX ...Summit demo got whoops from audience!  Had to let you know. --Joe", 0.0)))

    data.show()

    val regexTokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("Regex-Tokenized").setPattern("\\W")
    // alternatively .setPattern("\\w+").setGaps(false)

    regexTokenizer.transform(data).show()

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("features")


    val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1)

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, hashingTF, lsvc))

    val model = pipeline.fit(data.toDF())

    //   Prepare test documents, which are unlabeled.

    val test = spark.createDataFrame(Seq(
      TestFile(4L,  "OMG GET cheap stuff by sending money to"),
      TestFile(5L, "Hi Dad I started studying Spark the other"),
      TestFile(6L, "Congrats You have won a lottery"),
      TestFile(7L, "I am Doing Scala coding on Spark.It is a good platform for Big Data processing")))


    // Make predictions on test documents.
    model.transform(test.toDF()).show()

    spark.stop()


  }
  @BeanInfo
  case class LabelFile(id: Long, text: String, label: Double)


  @BeanInfo
  case class TestFile(id: Long, text: String)
}
