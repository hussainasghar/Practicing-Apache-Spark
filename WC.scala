
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.{rddToOrderedRDDFunctions, rddToPairRDDFunctions}
import org.apache.spark.{SparkConf, SparkContext}

  /* Created by hussain on 9/29/17.
  */
object WC {


    var sc: SparkContext = _
    def main(args: Array[String]): Unit = {


      if (args.length < 3) {
        println("Usage: SparkWordCount <input> <output> <numOutputFiles>")
        System.exit(1)
      }

      val inputArg0: (Unit => RDD[String]) = Unit => input(args(0))
      val processArg2: (RDD[String] => RDD[(String, Int)]) = process(_, args(2).toInt)
      val outputArg1: (RDD[(String, Int)] => Unit) = output(_, args(1))
      (inputArg0 andThen processArg2 andThen outputArg1) ()
      System.exit(0)


        sc.stop()

    }

    def input(inputFileName: String): RDD[String] = {
      val sparkConf = new SparkConf().setAppName("Spark WordCount")
      setSC(new SparkContext(sparkConf))
      sc.textFile(inputFileName)
    }

    def setSC(s: SparkContext): Unit = sc = s

    def process(rddIn: RDD[String], numTasks: Int): RDD[(String, Int)] = rddIn
      .flatMap(line => line.split("\\W+"))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _, numPartitions = numTasks)
      .sortByKey(ascending = true)

    def output(rdd: RDD[(String, Int)], outputFileName: String): Unit = rdd.saveAsTextFile(outputFileName)


}

