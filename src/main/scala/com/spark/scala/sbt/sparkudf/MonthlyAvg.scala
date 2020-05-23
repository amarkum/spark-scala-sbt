package com.spark.scala.sbt.sparkudf

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}

class MonthlyAvg() extends UserDefinedAggregateFunction {

  def inputSchema: StructType = StructType(Array(StructField("ts", TimestampType)))

  def bufferSchema = StructType(Array(
    StructField("totalBookings", IntegerType),
    StructField("distinctMonthCount", IntegerType),
    StructField("previousYear", IntegerType),
    StructField("currentYear", IntegerType),
    StructField("previousMonth", IntegerType),
    StructField("currentMonth", IntegerType),
  ))

  def dataType: DataType = DoubleType

  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0
    buffer(1) = 1

    // Year Buffer
    buffer(2) = 0
    buffer(3) = 0

    // Month Buffer
    buffer(4) = 0
    buffer(5) = 0

  }

  def update(buffer: MutableAggregationBuffer, input: Row) = {
    val dateString = input(0).toString()
    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    val zdt = ZonedDateTime.parse(dateString, dtf.withZone(ZoneId.systemDefault))

    if (buffer.getInt(5) == 0) {
      buffer(3) = zdt.getYear()
      buffer(5) = zdt.getMonthValue()
    }
    buffer(2) = buffer(3)
    buffer(3) = zdt.getYear()

    buffer(4) = buffer(5)
    buffer(5) = zdt.getMonthValue()

    if (buffer.getInt(5) != buffer.getInt(4) || buffer.getInt(3) != buffer.getInt(2)) {
      buffer(1) = buffer.getInt(1) + 1
    }
    buffer(0) = buffer.getInt(0) + 1
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer2.getInt(0)
    buffer1(1) = buffer2.getInt(1)
  }

  def evaluate(buffer: Row) = {
    buffer.getInt(0).asInstanceOf[Double] / buffer.getInt(1)
  }

}