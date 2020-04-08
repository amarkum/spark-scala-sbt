package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object MultiFileReadSupport {

  def main(args: Array[String]): Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from Multiple Sources")
      .getOrCreate()

    /**
     * Read fro various types of input and create a Data Frame
     */
    val dfCSV = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/csv/*")

    val dfParquet = sparkSession.read.options(Map("inferSchema" -> "true"))
      .parquet("src/main/resources/parquet/*")

    val dfORC = sparkSession.read.options(Map("inferSchema" -> "true"))
      .orc("src/main/resources/orc/*")

    /**
     * Print the Schema of the Data Frame and it's content.
     */
    dfCSV.printSchema()
    dfCSV.show()

    dfParquet.printSchema()
    dfParquet.show()

    dfORC.printSchema()
    dfORC.show()
  }
}
