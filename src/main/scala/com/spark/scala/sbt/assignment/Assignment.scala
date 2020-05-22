package com.spark.scala.sbt.assignment

import com.spark.scala.sbt.sparkudf.Count
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Problem Statement : Rapido
 *
 * 1. Calculate hourly, daily, weekly and monthly averages of aggregated counts of each
 * customer and make this accessible for querying purposes
 *
 * 2. Calculate Week-on-week cohorts for customers taking rides in that week, an example
 * cohort output is shown as follows.
 */
object Assignment {

  def main(args: Array[String]): Unit = {

    // Create a new UDAF from Count UDAF Class
    val Count = new Count()

    //Create a Spark Session
    val sparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val df = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/rapido/dataset/rapido-data.csv")

    val filtered_df = df.groupBy("number")
      .agg(Count(df.col("ts")).alias("Timestamp"))

    filtered_df.printSchema()
    filtered_df.show(20, false)
//    filtered_df.show(df.count.toInt, false)
  }
}
