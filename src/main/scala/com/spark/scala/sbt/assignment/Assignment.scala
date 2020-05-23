package com.spark.scala.sbt.assignment

import com.spark.scala.sbt.sparkudf.{HourlyAvg, MonthlyAvg, WeeklyAvg, YearlyAvg}
import org.apache.spark.sql.SparkSession

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

    val YearlyAvg = new YearlyAvg()
    val MonthlyAvg = new MonthlyAvg()
    val WeeklyAvg = new WeeklyAvg()
    val HourlyAvg = new HourlyAvg()

    val sparkSession = SparkSession.builder().master("local")
      .getOrCreate()

    val df = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/rapido/dataset/rapido-data.csv")

    val filtered_df = df.groupBy("number")
      .agg(
        YearlyAvg(df.col("ts")).alias("Yearly Average"),
        MonthlyAvg(df.col("ts")).alias("Monthly Average"),
        WeeklyAvg(df.col("ts")).alias("Weekly Average"),
        HourlyAvg(df.col("ts")).alias("Hourly Average"),
      )

    filtered_df.printSchema()
    filtered_df.show(20, false)
    // filtered_df.show(df.count.toInt, false)
  }
}
