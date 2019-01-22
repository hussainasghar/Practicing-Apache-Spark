package dataframe

import org.apache.spark.sql.SparkSession

object Json_DataSets {

  //Set multiline to true if json Datset is multiLine


  val spark = SparkSession.builder()
    .master("local[*]").appName("DataFrame").
    config("spark.master", "local").getOrCreate()


  // Primitive types (Int, String, etc) and Product types (case classes) encoders are
  // supported by importing this when creating a Dataset.
  import spark.implicits._

  // A JSON dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files
  val path = "examples/src/main/resources/people.json"
  val peopleDF = spark.read.json(path)

  // The inferred schema can be visualized using the printSchema() method
  peopleDF.printSchema()
  // root
  //  |-- age: long (nullable = true)
  //  |-- name: string (nullable = true)

  // Creates a temporary view using the DataFrame
  peopleDF.createOrReplaceTempView("people")

  // SQL statements can be run by using the sql methods provided by spark
  val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
  teenagerNamesDF.show()
  // +------+
  // |  name|
  // +------+
  // |Justin|
  // +------+

  // Alternatively, a DataFrame can be created for a JSON dataset represented by
  // a Dataset[String] storing one JSON object per string
  val otherPeopleDataset = spark.createDataset(
    """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
  val otherPeople = spark.read.json(otherPeopleDataset)
  otherPeople.show()
  // +---------------+----+
  // |        address|name|
  // +---------------+----+
  // |[Columbus,Ohio]| Yin|
  // +---------------+----+


}
