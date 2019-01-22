package dataframe

import org.apache.spark.sql.SparkSession

object API_DataSources {

  val spark = SparkSession.builder()
    .master("local[*]").appName("DataFrame").
    config("spark.master", "local").getOrCreate()


 /** Generic Load/Save Functions *******/

  val usersDF = spark.read.load("examples/src/main/resources/users.parquet")

  usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

 /***** Manually Specifying Options *******/

 val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
  peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")

 /** Run SQL on files directly ***/

 val sqlDF = spark.sql("SELECT * FROM parquet.'examples/src/main/resources/users.parquet'")






}
