package com.spark.scala.sbt.dataset

import org.apache.spark.sql.SparkSession

case class Ratings(director_name: String, duration: Integer, movie_title: String, country: String, imdb_score: Double)

object DataSetBasic {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("DataSet Basics")
      .master("local")
      .getOrCreate()

    val dataFrame = sparkSession.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("src/main/resources/dataset/csv/*")

    val filteredDF = dataFrame.select("director_name", "duration", "movie_title", "country", "imdb_score")
    filteredDF.write
      .option("header", "true")
      .mode("overwrite")
      .csv("src/main/resources/dataset/exports/movies/")

    import sparkSession.implicits._

    val movieDataSet = filteredDF.as[Ratings]

    movieDataSet.show()
  }

}
