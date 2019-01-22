package streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Mix1 {

  def main(args: Array[String]): Unit = {

  //  Setting up a driver that can recover from failure in Scala

  def createStreamingContext():StreamingContext = {

    val conf = new SparkConf().setMaster("local[*]")
  //  val sc = new SparkContext(conf)

    val ssc = new StreamingContext(conf, Seconds(1))

    val checkpointDirectory = "srcOfDir"

    ssc.checkpoint(checkpointDirectory)

    ssc

  }


    

    // Get StreamingContext from checkpoint data or create a new one
    val context = StreamingContext.getOrCreate( checkpointPath = "$checkpointDirectory", createStreamingContext _)


    context.start()
    context.awaitTermination()

   // Launching a driver in supervise mode
  //  ./bin/spark-submit --deploy-mode cluster --supervise --master spark://... App.jar



  }

}
