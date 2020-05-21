package com.spark.scala.sbt.assignment

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

    //Create a Spark Session
    val sparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val df = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/ct_rr.csv")

    val filtered_df = df.groupBy("number")
      .agg(collect_list("ts").alias("Timestamp"))

    filtered_df.printSchema()
    filtered_df.show(df.count.toInt, false)
  }
}
