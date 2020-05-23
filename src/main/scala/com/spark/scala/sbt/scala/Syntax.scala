package com.spark.scala.sbt.scala

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

object Syntax {

  def main(args: Array[String]): Unit = {

    /**
     * 1. Implicits
     */
    implicit val myImplicitInt = 100

    def sendGreetings(toWhom: String)(implicit howMany: Int) =
      println(s"Hello $toWhom, $howMany blessings to you and yours!")

    sendGreetings("John")(1000)
    sendGreetings("Jane")

    /**
     * 2. Date and Time
     */
    val dateString = "2020-05-25 09:15:31.0"

    val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")
    val zdt = ZonedDateTime.parse(dateString, dtf.withZone(ZoneId.systemDefault))

    System.out.println(zdt.getHour)

    /**
     * 3. Functions
     */

    // method to find sum
    def findSum(a: Int, b: Int): Int = {
      a + b
    }

    // call out the method findSum
    println(findSum(3, 4))

    // Since, the value can easily be computer, we don't write the curly brace
    def findDifference(a: Int, b: Int): Int = a - b

    // call out the difference method
    println(findDifference(8, 4))

    // method can Infer return type as well
    def findPower(a : Int,  b : Int) = math.pow(a,b)

    // find the power
    println("power of 2 & 2 = "+findPower(2,2))

    /**
     * 4. Functional Programming
     */

    val square : Int  => Int = _*10

    List(1,2,4,5) map square foreach println

    // Anonymous functions can be used instead of named functions:
    List(1, 2, 3) map (x => x + 10)

    // And the underscore symbol, can be used if there is just one argument to the
    // anonymous function. It gets bound as the variable
    List(1, 2, 3) map (_ + 10)
  }

}
