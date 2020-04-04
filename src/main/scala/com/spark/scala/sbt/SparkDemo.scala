package com.spark.scala.sbt

import org.apache.spark.{SparkConf, SparkContext}
object SparkDemo {

  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local").setAppName("DataFrame with Spark")
    val sc = new SparkContext(sparkConf)
  }
}