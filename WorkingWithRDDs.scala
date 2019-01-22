package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 10/1/17.
  */
object WorkingWithRDDs {

  def main(args: Array[String]): Unit = {

    val inputFile = "src/main/resources/input"
    val outputFile = "src/main/resources/actiona1"

    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    val input =  sc.textFile(inputFile)

    val nums = sc.parallelize(List(1, 2,3,4,5,61,1,111,1,2,5,4,3,2,4,5,6,7,5))

    nums.flatMap(x => 1 to x)

    val pets = sc.parallelize(List(("cat", 1), ("dog", 1), ("cat", 2)))

    pets.groupByKey() // => {(cat, 3), (dog, 1)}
    pets.groupByKey() // => {(cat, Seq(1, 2)), (dog, Seq(1)}
    pets.sortByKey() // => {(cat, 1), (cat, 2), (dog, 1)}

    pets.saveAsTextFile(outputFile)

  }
}
