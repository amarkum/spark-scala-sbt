package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CustomSchema {

  def main(args : Array[String]): Unit ={

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Custom Schema for Data Frame")
      .getOrCreate()

    // Create a Custom Schema
    val customSchema = StructType(
      StructField("Location", StringType, true)
        :: StructField("City", StringType, true)
        :: StructField("Organization", StringType, true)
        :: StructField("FirstName", StringType, true)
        :: StructField("LastName", StringType, true)
        :: StructField("Country", StringType, true)
        :: StructField("Reseller", StringType, true)
        :: StructField("State", StringType, true)
        :: StructField("Zipcode", IntegerType, true)
        :: Nil
    )

    //Impose Custom Schema to Data Frame
      val dataFrame = sparkSession.read
        .schema(customSchema)
        .options(Map("header"->"true"))
        .csv("src/main/resources/csv/*")

    dataFrame.printSchema()
    dataFrame.show()
  }

}
