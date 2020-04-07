package com.spark.scala.sbt

import org.apache.spark.sql.SparkSession

object DataFrameMethods {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Custom Schema for Data Frame")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/csv/*")

    dataFrame.printSchema()

    // .schema() method gets the schema of the data frame
    val schema = dataFrame.schema
    print(schema)

    // .dtypes() method gets the column and data type
    val colAndTypes = dataFrame.dtypes
    colAndTypes.foreach(println)

    // .describe() method gives describe a specific column
    val zipDescription = dataFrame.describe("Zip")
    zipDescription.show()

    // .head() method gets the first n rows and save it
    dataFrame.head(5).foreach(println)

  }

}
