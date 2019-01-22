package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 10/1/17.
  */
object WorkingWithKeyValuePairs {

  def main(args: Array[String]): Unit = {


    val inputFile = "src/main/resources/input"
    val outputFile = "src/main/resources/actiona1"

    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
  //  val input =  sc.textFile(inputFile)

    val visits = sc.parallelize(List(
      ("index.html", "1.2.3.4"), ("about.html", "3.4.5.6"), ("index.html", "1.3.3.1")))

    val pageNames = sc.parallelize(List(("index.html", "Home"), ("about.html", "About")))

    visits.join(pageNames)
    // (“index.html”, (“1.2.3.4”, “Home”))
    // (“index.html”, (“1.3.3.1”, “Home”))
    // (“about.html”, (“3.4.5.6”, “About”))
    visits.cogroup(pageNames)
    // (“index.html”, (Seq(“1.2.3.4”, “1.3.3.1”), Seq(“Home”)))
    // (“about.html”, (Seq(“3.4.5.6”), Seq(“About”)))
  }
}
