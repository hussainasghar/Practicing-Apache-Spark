package dataframe

import org.apache.spark.sql.SparkSession

object Schema_Merging_API {

  /********************************** Schema Merging ********************************************/

  val spark = SparkSession.builder()
    .master("local[*]").appName("DataFrame").
    config("spark.master", "local").getOrCreate()

  // This is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._

  // Create a simple DataFrame, store into a partition directory
  val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")

  squaresDF.write.parquet("data/test_table/key=1")

  // Create another DataFrame in a new partition directory,
  // adding a new column and dropping an existing column
  val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
  cubesDF.write.parquet("data/test_table/key=2")

  // Read the partitioned table
  val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
  mergedDF.printSchema()



}
