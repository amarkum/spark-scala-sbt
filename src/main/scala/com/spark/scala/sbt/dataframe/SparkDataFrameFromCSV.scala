package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit}

object SparkDataFrameFromCSV {

  def main(args: Array[String]): Unit = {

    // Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from CSV")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/dataframe/csv/customer.csv")

    // To concatenate columns we can use concat method, we need to import org.apache.spark.sql.functions.{concat, lit}
    val concatDf = df.select(concat($"Region", lit(" & "), $"City").as("regionCity"))
    concatDf.show(false)

    /**
     * Partitioning
     */
    // we can use partitionBy() method to partition the dataframe by a single or multiple column.
    df.write.partitionBy("Region","City").format("csv").save("namesPartByColor.parquet")

    /**
     * Expression Evaluation - $
     */
    // To use "$" in-front of a column name, as evaluation operator we must import implicits
    // select will select only the required columns
    val selectedDF = df.select("Address", "Reseller")

    // select will select only the required columns, $ is mandatory for all columns.
    val filteredDF = selectedDF.select($"Address", ($"Reseller"+3).as("Reseller"))

    /**
     * Filtered Dataframe
     */
    filteredDF.printSchema()
    filteredDF.show()

    // filter using $ | using select will fill in with the expression check.
    val limitedDF = filteredDF.filter($"Reseller"<4)

    // createOrReplaceTempView is used to create a SQL kind of table
    limitedDF.createOrReplaceTempView("resller")

    val sqlDF = sparkSession.sql("SELECT * FROM resller")

    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("resell")

    // Global temporary view is tied to a system preserved database `global_temp`sqlDF.show()
    sparkSession.sql("SELECT * FROM global_temp.resell").show()
  }
}
