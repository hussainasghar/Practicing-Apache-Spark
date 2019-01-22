package dataframe

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.sql.SparkSession


object Jason_DataFrame {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[*]").appName("DataFrame").
      config("spark.master", "local").getOrCreate()

    val df = spark.read.format("json").load("json/zips.json")

      df.printSchema

    df.filter("_id = 55105").show

    // Converting the DataFrame to Parquet format, and then querying it as a Hive table
    val options = Map("path" -> "/user/hive/warehouse/zipcodes")

    df.select("*").write.format("parquet").options(options).saveAsTable("zipcodes")

//   hive> DESCRIBE zipcodes

//  hive>  SELECT city FROM zipcodes WHERE (`_id` == '55105');








  }
}
