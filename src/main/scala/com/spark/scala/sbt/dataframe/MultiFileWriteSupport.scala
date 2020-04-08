package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object MultiFileWriteSupport {

  def main(args: Array[String]): Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Export DataFrame to Multiple Format")
      .getOrCreate()

    /**
     * Create a Data Frame by reading type of File
     */
    val dfCSV = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/csv/*")

    dfCSV.printSchema()
    dfCSV.show()

    /**
     * Convert to any file format of your choice
     */
    // to ORC
    dfCSV.write
      .option("compression","none")
      .mode("overwrite")
      .orc("src/main/resources/dataframe/orc/")

    // to Parquet
    dfCSV.write
      .option("compression","none")
      .mode("overwrite")
      .parquet("src/main/resources/dataframe/parquet/")

    // to JSON
    dfCSV.write
      .option("compression","none")
      .mode("overwrite")
      .json("src/main/resources/dataframe/json/")

    // to AVRO
    dfCSV.write
      .format("avro")
      .mode("overwrite")
      .save("src/main/resources/dataframe/avro/")

  }
}
