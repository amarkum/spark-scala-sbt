package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object RDDTransformation {

  def main(args: Array[String]): Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from Avro Source")
      .getOrCreate()

    // Using map() for each line of the file perform some opertaion
    val rddData = sparkSession.read.textFile("src/main/resources/data/sample.txt").rdd
    val lineMap = rddData.map(line => (line, line.length))
    lineMap.foreach(println)
  }
}
