package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSplitColumn {
  def main(args: Array[String]): Unit = {

    //Create a Spark Session
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark DF from Avro Source")
      .getOrCreate()

    val customSchema = StructType(
      StructField("catg", StringType, true)
        :: StructField("sub_catg", StringType, true)
        :: StructField("doc_name", StringType, true)
        :: StructField("revision_label", StringType, true)
        :: StructField("extension", StringType, true)
        :: Nil
    )

    //catg,sub_catg,doc_name,revision_label,extension
    val df = sparkSession.read
      .format("csv")
      .schema(customSchema)
      .option("delimiter", "_")
      .load("src/main/resources/data/sample.txt")

    df.show()
  }
}
