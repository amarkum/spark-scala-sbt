package com.spark.scala.sbt

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
object SparkBasicDataFrame1x {

  /**
   * In Spark 1.x DataFrame is created using SQLContext
   * SQLContext is created using SparkContext
   * @param args
   */
  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("DataFrame with Spark")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val rdd = sparkContext.parallelize(Array(1,2,3,4,5))
    val schema = StructType(
      StructField("Numbers", IntegerType,false):: Nil
    )
    val rowRdd = rdd.map(line => Row(line))
    val df = sqlContext.createDataFrame(rowRdd, schema)
    df.printSchema()
    df.show()
  }
}