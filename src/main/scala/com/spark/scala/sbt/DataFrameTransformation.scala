package com.spark.scala.sbt

import org.apache.spark.sql.SparkSession

object DataFrameTransformation {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Custom Schema for Data Frame")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/csv/*")

    // .select() is used to select specific columns from a Data Frame
    val selectedDF = dataFrame.select("FirstName","Company")
    selectedDF.printSchema()
    selectedDF.show(10)

    // .select with .where() - it is used to select & display records based on some condition
    val whereSelectDF = dataFrame
      .select("FirstName","Company")
      .where("Region == 'USA East'")

    whereSelectDF.printSchema()
    whereSelectDF.show()

    // .select with .filter() - it is used to select & display records based on filter
    val filterSelectDF = dataFrame
      .select("FirstName","Company")
      .filter(dataFrame("Region") === "USA East")

    filterSelectDF.show()

    // .select() with .groupBy() - it is used to group columns
    val groupByDF = dataFrame
      .select("FirstName", "Company", "Region")
      .groupBy("Region").count()

    groupByDF.show()

  }

}
