package com.spark.scala.sbt.sparkudf

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

//Extend UserDefinedAggregateFunction to write custom aggregate function
//You can also specify any constructor arguments. For instance you
//can have CustomMean(arg1: Int, arg2: String)
class YearlyAvg() extends UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("ts", TimestampType)))

  // Intermediate Schema
  def bufferSchema = StructType(Array(
    StructField("size", IntegerType),
    StructField("sumUp", IntegerType),
    StructField("prevY", IntegerType),
    StructField("currY", IntegerType),
    StructField("hourIndex", IntegerType)
  ))


  def dataType: DataType = IntegerType

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0
    buffer(1) = 0
    buffer(2) = 0
    buffer(3) = 0
    buffer(4) = 1
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val dateString = input(0).toString()
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    val zdt = ZonedDateTime.parse(dateString, dtf.withZone(ZoneId.systemDefault))

    if(buffer.getInt(3) == 0){
      buffer(3) = zdt.getYear()
    }
    buffer(2) = buffer(3)
    buffer(3) = zdt.getYear()

    if(buffer.getInt(2) != buffer.getInt(3)){
      buffer(4) = buffer.getInt(4) + 1
      buffer(1)  = 1
    }
    else{
      buffer(1) = buffer.getInt(1)  + 1
    }
    buffer(0) = buffer.getInt(0) + 1
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer2.getInt(0)
    buffer1(4) = buffer2.getInt(4)
  }

  def evaluate(buffer: Row) = {
    buffer.getInt(0)/buffer.getInt(4)
  }

}