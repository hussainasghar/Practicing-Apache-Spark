package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hussain on 10/15/17.
  */
object Fiverr_CAP_Lab7 {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("CAP").setMaster("local[*]")
    val sc = new SparkContext(conf)

   //   val rdd =   sc.textFile("/loudacre/weblogs/*6")
  //    val rdd1 = sc.textFile("/loudacre/partmfiles/*")
    val OutputFile = "src/main/resources/outputCheck"

      val rdd = sc.textFile("src/main/resources/weblogs/*6.log")
     // val rdd1 = sc.textFile("src/main/resources/partmfiles/*")



    /********** 1-a) ********************/

      def pairRDD(rdd:RDD[String]) ={
        rdd.flatMap((delimeter=>delimeter.split(" ").map(userId=>(userId,1))))
      }
  //  val pairRdd  = rdd.map(line => line.split(" ")).map(userId=>(userId,1)).foreach(println)
    val pairRdd  = rdd.flatMap(delimeter=>delimeter.split(" ")).map(userId=>(userId,1))
    /********** 1-b)  ********************/


    val reducee = pairRdd.reduceByKey(_ + _).foreach(println)

   // reducee.saveAsTextFile(OutputFile)


   /* /******************** 2-a) ********************/
    val pairRdd1 = rdd.flatMap(line => line.split(" ")).map(userId=>(1,userId))
    /******************** 2-b) ********************/
    val count = pairRdd1.countByKey().foreach(println)


    /******************** 3-a) ********************/
   val pairs = rdd.flatMap(line => line.split(" ")).map(userId => (userId.split(" ")(0), userId))
    /******************** 3-b) ********************/
   val groupp = pairs.groupByKey.foreach(println)


    /******************** 4-a) ********************/
    val lol = rdd1.map(x => (x.split(",")(0),Array(x.split(",")(1))))
    /******************** 4-b) ********************/
    val reducee1 = pairRdd.reduceByKey(_ + _)
    val joinn = lol.join(reducee1)

    /******************** 4-c) ********************/

    val pairRdds  = rdd.flatMap(line => line.split(" ")).map(userId=>(userId,1)).reduceByKey(_+_)
    val zip = rdd1.flatMap(x=>x.split(" ")).zipWithIndex()
    val indexswap = zip.map{ case (x,y)=> (y,x) }
    val fn = indexswap.lookup(3).take(5).foreach(println)
    val ln = indexswap.lookup(4).take(5).foreach(println)*/

  //  val a = pairRdds.groupByKey(fn)

   //  val ans = pairRdds.join(fn)
  //    val anss = pairRdds.join(ln)





  }


}
