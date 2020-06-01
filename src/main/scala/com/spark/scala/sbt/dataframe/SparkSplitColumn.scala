package com.spark.scala.sbt.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

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

   val ndf =  df.withColumn("doc_name", concat_ws("_", col("doc_name"), col("revision_label")))
      .withColumn("extension", substring_index(col("revision_label"), ".", -1))
      .withColumn("revision_label", regexp_extract(col("revision_label"),"""\d+""", 0))

     ndf.show(false)
  }
}
