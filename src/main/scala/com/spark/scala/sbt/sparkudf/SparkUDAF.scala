package com.spark.scala.sbt.sparkudf

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
class Count() extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("ts", TimestampType)))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("count", LongType)
  ))

  // Returned Data Type .
  def dataType: DataType = LongType

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0L // set the average to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val dateString = input(0).toString()

    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    val zdt = ZonedDateTime.parse(dateString, dtf.withZone(ZoneId.systemDefault))
    println(zdt.toLocalDateTime)

    buffer(0) = buffer.getLong(0) + 1
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer2.getLong(0)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getLong(0).toLong
  }


}