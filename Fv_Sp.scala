package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 11/25/17.
  */
object Fv_Sp {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark").setMaster("local")
    val sc = new SparkContext(conf)


    val OutputFile = "src/main/resources/outputCheck"

    val rdd = sc.textFile("src/main/resources/LogFile.log")

    val split  = rdd.flatMap(delimeter=>delimeter.split(" "))
    val pairRdd = split.map(userId=>(userId,1))
    val reducee = pairRdd.reduceByKey(_ + _)
    val printt =  reducee.foreach(println)




    /////////////////////////////////////////////////////////////////////////////////////////////////


   /* val rdd1 = sc.textFile("src/main/resources/graphs/1.txt")

    val nodes = rdd1.flatMap(delimeter=>delimeter.split(" "))
    val pairs = nodes.map(x=>(x,1))
    val counts = pairs.reduceByKey((a,b)=>a+b)

    if(isEven(counts.collect()))
      println("Graph have Euler Tour")
      else
        println("Graph does not have Euler Tour")



   def isEven(numbers:Int):Boolean={
    for (num <- numbers){
    if (num._1 %2 == 0)
    true
    else
    false
    }
    }
*/


  }
}
