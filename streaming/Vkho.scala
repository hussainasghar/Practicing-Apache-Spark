package streaming

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.hadoop.hdfs.server.datanode.DataStorage
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Vkho {

  def main(args: Array[String]): Unit = {

    if(args.length<2){
      System.err.print("Usage: Mix <host> <port> ")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local[*").setAppName("Try")
  //  val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf,Seconds(1))

    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))

    words.foreachRDD{(rdd:RDD[String],time:Time)=>

      val spark  = SparkSingletonSeasson.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._

      val wordsDataFrame = rdd.map(w=> Record(w)).toDF()

      wordsDataFrame.createOrReplaceTempView("words")

  //    val wordCounts = spark.sql("select word,count(*) as totalWords from words group by word")
   //   wordCounts.show()



    }

    ssc.start()
    ssc.awaitTermination()



  }



}

case class Record(words:String)

object SparkSingletonSeasson{

 @transient private var instance:SparkSession = _

  def getInstance(sparkConf:SparkConf):SparkSession={
  if(instance == null){
    instance = SparkSession.builder().config(sparkConf).getOrCreate()
  }

    instance




  }
}