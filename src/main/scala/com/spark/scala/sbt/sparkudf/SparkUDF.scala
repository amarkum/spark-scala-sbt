package com.spark.scala.sbt.sparkudf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkUDF {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("DataSet Basics")
      .master("local")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/dataset/csv/*")

    /**
     * Create an UDF object, pass the return type and passed parameter to [] brackets
     * Supply the method to call for the UDF in ()
     */
    val upperCaseUDF = udf[String, String](toUpperCase)

    dataFrame.select(upperCaseUDF(dataFrame("director_name")).as("DIRECTOR"),
      dataFrame("movie_title").as("MOVIE"))
      .show()

  }

  /*
   * Create a Custom UDF method
   */
  def toUpperCase(s: String): String = s.toUpperCase()
}
