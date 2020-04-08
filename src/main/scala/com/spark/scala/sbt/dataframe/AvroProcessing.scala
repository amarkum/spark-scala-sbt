package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession

object AvroProcessing {

  def main(args: Array[String]): Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from Avro Source")
      .getOrCreate()

    //Read avro file and create a Data Frame
    val avroDF = sparkSession.read
      .format("avro")
      .load("src/main/resources/avro/*")

    avroDF.printSchema()
    avroDF.show()

    // Save the avro data into CSV
    avroDF.write
      .option("compression","none")
      .mode("overwrite")
      .csv("src/main/resources/avro-csv")
  }

}
