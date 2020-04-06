package com.spark.scala.sbt

import org.apache.spark.sql.SparkSession

object SparkDataFrameFromCSV {

  def main(args: Array[String]) : Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from CSV")
      .getOrCreate()

    val df = sparkSession.read.csv("src/main/resources/csv/customer.csv")
    df.show()
  }
}
