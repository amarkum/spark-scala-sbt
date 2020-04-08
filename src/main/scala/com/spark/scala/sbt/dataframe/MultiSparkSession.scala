package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object MultiSparkSession {

  def main(args: Array[String]): Unit = {

    // Create 2 SparkSession
    val sparkSessionOne = SparkSession.builder()
      .master("local")
      .appName("Spark Session One")
      .getOrCreate()

    val sparkSessionTwo = SparkSession.builder()
      .master("local")
      .appName("Spark Session Two")
      .getOrCreate()

    // Create two RDD with different SparkSession
    val rddOne = sparkSessionOne.sparkContext.parallelize(Array(1, 2, 3, 4, 5))
    val rddTwo = sparkSessionTwo.sparkContext.parallelize(Array(6, 7, 8, 9, 10))

    // Print the value
    rddOne.collect().foreach(println)
    rddTwo.collect().foreach(println)
  }
}
