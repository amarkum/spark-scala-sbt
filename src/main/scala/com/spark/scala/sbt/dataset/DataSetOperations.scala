package com.spark.scala.sbt.dataset

import org.apache.spark.sql.SparkSession

object DataSetOperations {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("DataSet Basics")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._

    // as Ratings class is in the same package, it can identify it.
    val dataSet = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/dataset/exports/movies/*").as[Ratings]

    // we can easily perform .filter() operation on a Data Set,
    // as it will not led to change in the column numbers, and hence returns a Sub DataSet
    val filterDataSet = dataSet
      .filter(movieObject => movieObject.duration > 190)
      .orderBy("movie_title")

    filterDataSet.show()
    println("No. of records : " + filterDataSet.count())

    // we can use .where() method as well to filter out records
    val whereDataSet = dataSet
      .where(dataSet("imdb_score") > 7.0).where(dataSet("duration") > 200)
      .orderBy("movie_title")

    whereDataSet.show()

    // we can not use .select() method in data set as it will lead to change in Column
    // and we have to define a new class at first. The select returns a data frame in data set
    // making use of .as[class] will create a new data set.

  }

}
