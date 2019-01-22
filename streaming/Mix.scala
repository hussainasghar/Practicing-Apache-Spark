package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Mix {

  def main(args: Array[String]): Unit = {

    /* val ssc = StreamingContext.getOrCreate(checkpointDirectory, createStreamingContext _)

                    OR

    val ssc = StreamingContext.getOrCreate(checkpointPath = " ",createStreamingContext _)*/

    // Launching a driver in supervise mode
    //  ./bin/spark-submit --deploy-mode cluster --supervise --master spark://... App.jar



    //For testing data
    // val testing = streamingContext.queueStream(queue = queuehere)

    //  val lines = streamingContext.textFileStream("src/main/resources/ham.txt")

    //   val lines = streamingContext.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)


    /* if (args.length < 2) {
       System.err.println("Usage: NetworkWordCount <hostname> <port>")
       System.exit(1)
     }*/

    /*val conf = new SparkConf().setAppName("Simple-Spark-Streaming").setMaster(master = "local[*]")

    //   val sc = new SparkContext(conf)

    val streamingContext = new StreamingContext(conf,Seconds(1))

    // Create a DStream using data received after connecting to port  on the localhost

    val lines = streamingContext.socketTextStream("localhost",9999)



    val filterdLines = lines.filter(_.contains("I"))

    filterdLines.print()


    // Start our streaming context and wait for it to "finish"
    streamingContext.start()
    // Wait for the job to finish
    streamingContext.awaitTermination()*/

    // streamingContext.stop()

    /*def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = 45  // add the new values with the previous running count to get the new count
      Some(newCount)
    }

    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)*/

    /*val spamInfoRDD = ssc.sparkContext.newAPIHadoopRDD(...) // RDD containing spam information

    val cleanedDStream = wordCounts.transform { rdd =>
      rdd.join(spamInfoRDD).filter(...) // join data stream with spam information to do data cleaning
      ...
    }*/

    // Reduce last 30 seconds of data, every 10 seconds       30 seconds is Window-Length and 10 Seconds is Interval
    // val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))

    /*  dstream.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          val connection = createNewConnection()
          partitionOfRecords.foreach(record => connection.send(record))
          connection.close()
        }
      }

      dstream.foreachRDD { rdd =>
    rdd.foreachPartition { partitionOfRecords =>
      // ConnectionPool is a static, lazily initialized pool of connections
      val connection = ConnectionPool.getConnection()
      partitionOfRecords.foreach(record => connection.send(record))
      ConnectionPool.returnConnection(connection)  // return to the pool for future reuse
    }
  }

      */




    /*val outlierDStream = accessLogsDStream.transform { rdd =>
      extractOutliers(rdd)
    }*/

    //   streamingContext.checkpoint("hdfs://...")


    //use window() to count data over a window
    /* val accessLogsWindow = accessLogsDStream.window(Seconds(30), Seconds(10))
     val windowCounts = accessLogsWindow.count()*/



    /*  val ipDStream = accessLogsDStream.map(logEntry => (logEntry.getIpAddress(), 1))
      val ipCountDStream = ipDStream.reduceByKeyAndWindow(
        {(x, y) => x + y}, // Adding elements in the new batches entering the window
        {(x, y) => x - y}, // Removing elements from the oldest batches exiting the window
        Seconds(30),
        // Window duration
        Seconds(10))
      // Slide duration*/


    /*  val ipDStream = accessLogsDStream.map{entry => entry.getIpAddress()}
      val ipAddressRequestCount = ipDStream.countByValueAndWindow(Seconds(30), Seconds(10))
      val requestCount = accessLogsDStream.countByWindow(Seconds(30), Seconds(10))*/

    //  Running count of response codes using updateStateByKey()
    /*  def updateRunningSum(values: Seq[Long], state: Option[Long]) = {
        Some(state.getOrElse(0L) + values.size)
      }
      val responseCodeDStream = accessLogsDStream.map(log => (log.getResponseCode(), 1L))
      val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum _)
  */

    /*   ipAddressRequestCount.saveAsTextFiles("outputDir", "txt")*/

    //   Saving SequenceFiles from a DStream in Scala
    /* val writableIpAddressRequestCount = ipAddressRequestCount.map {
       (ip, count) => (new Text(ip), new LongWritable(count)) }
     writableIpAddressRequestCount.saveAsHadoopFiles[
       SequenceFileOutputFormat[Text, LongWritable]]("outputDir", "txt")


   // Saving data to external systems with foreachRDD()
       ipAddressRequestCount.foreachRDD { rdd =>
 rdd.foreachPartition { partition =>
 // Open connection to storage system (e.g. a database connection)
 partition.foreach { item =>
 // Use connection to push item to system
 }
 // Close connection
 }
 }


         //Streaming text files written to a directory in Scala
          val logData = streamingContext.textFileStream(logDirectory)

  //    Streaming SequenceFiles written to a directory in Scala

          streamingContext.fileStream[LongWritable, IntWritable,
 SequenceFileInputFormat[LongWritable, IntWritable]](inputDirectory).map {
 case (x, y) => (x.get(), y.get())
 }

 ////  Apache Kafka subscribing to Panda’s topic in Scala

 import org.apache.spark.streaming.kafka._

 // Create a map of topics to number of receiver threads to use
 val topics = List(("pandas", 1), ("logs", 1)).toMap
 val topicLines = KafkaUtils.createStream(streamingContext, zkQuorum, group, topics)
 StreamingLogInput.processLines(topicLines.map(_._2))

  // FlumeUtils agent in Scala
 val events = FlumeUtils.createStream(streamingContext, receiverHostname, receiverPort)

 FlumeUtils custom sink in Scala
 val events = FlumeUtils.createPollingStream(streamingContext, receiverHostname, receiverPort)

 // Assuming that our flume events are UTF-8 log lines
 val lines = events.map{e => new String(e.event.getBody().array(), "UTF-8")}

 //Storage System
   HDFS or Amazon S3



 */







  }
}
