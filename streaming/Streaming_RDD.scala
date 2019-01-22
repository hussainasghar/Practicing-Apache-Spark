package streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Streaming_RDD {

  def main(args: Array[String]): Unit = {

    /*if (args.length < 2) {
   System.err.println("Usage: NetworkWordCount <hostname> <port>")
   System.exit(1)
 }
*/

   val conf = new SparkConf().setAppName("Simple-Spark-Streaming").setMaster(master = "local[*]")

 //   val sc = new SparkContext(conf)

    val streamingContext = new StreamingContext(conf,Seconds(1))

    // Create a DStream using data received after connecting to port  on the localhost

    val lines = streamingContext.socketTextStream("localhost",9999)


    val filterdLines = lines.filter(_.contains("I"))

    filterdLines.print()

    // Start our streaming context and wait for it to "finish"
    streamingContext.start()
    // Wait for the job to finish
    streamingContext.awaitTermination()



  }
}
