package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import sqlContext.implicits._

/**
  * Created by hussain on 10/26/17.
  */
object List_DataFrame {

  case class Stock(exchange: String, symbol: String, date: String, open: Float, high:
  Float, low: Float, close: Float, volume: Integer, adjClose: Float)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[*]").appName("DataFrame").
      config("spark.master", "local").getOrCreate()


    val stocks = List(
      "NYSE,BGY,2010-02-08,10.25,10.39,9.94,10.28,600900,10.28",
      "NYSE,AEA,2010-02-08,4.42,4.42,4.21,4.24,205500,4.24",
      "NYSE,CLI,2010-02-12,30.77,31.30,30.63,31.30,1020500,31.30"
                     )

    val Stocks = stocks.map(_.split(",")).
      map(x=>Stock(x(0),x(1),x(2),x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toFloat,x(7).toInt,x(8).toFloat))

     val stocksDF = spark.createDataFrame(Stocks)

    stocksDF.count

    stocksDF.show

    stocksDF.first

    stocksDF.printSchema

    stocksDF.groupBy("date").count.filter("count > 1").rdd.collect

    stocksDF.registerTempTable("stock")

    spark.sql("SELECT symbol, close FROM stock WHERE close > 5 ORDER BY symbol").show








  }
}
