package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object SparkTable {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Table using Data Frame")
      .master("local")
      .getOrCreate()

    val dataFrame = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/dataframe/csv/*")

    dataFrame.show()

    // .createTempView() -  is used to create a temporary table from the data frame
    dataFrame.createTempView("customer_table")

    // sparkSession.sql() - is used to write an SQL query on the tables
    val filteredDataFrame = sparkSession.sql("select * from customer_table where Region = 'USA West'")

    // .createOrReplaceTempView() can overwrite already existing table
    filteredDataFrame.createOrReplaceTempView("customer_table")

    sparkSession.sql("select * from customer_table").show()
  }

}
