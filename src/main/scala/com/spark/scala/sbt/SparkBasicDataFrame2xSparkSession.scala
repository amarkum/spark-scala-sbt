package com.spark.scala.sbt

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Spark 2.X
 */
object SparkBasicDataFrame2xSparkSession {

  def main(args: Array[String]) : Unit = {
    // Create a Spark Session, object is returned by Builder class
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark 2 DataFrame")
      .getOrCreate()

    // Create the RDD by utilizing SparkContext of SparkSession
    val rdd = sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4, 5))

    // Create an Schema
    val schema = StructType(
      StructField("Numbers", IntegerType, false) :: Nil
    )

    // Create a specific Row RDD from RDD
    val rowRdd = rdd.map(line => Row(line))

    // Create a df from createDataFrame of sparkSession
    val df = sparkSession.createDataFrame(rowRdd, schema)

    //Show the schema
    df.printSchema()

    //show the content of df
    df.show()
  }
}
