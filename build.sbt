 name := "spark-demo"
 version := "1.0"
 scalaVersion := "2.12.11"

 // https://mvnrepository.com/artifact/org.apache.poi/poi
 libraryDependencies += "org.apache.poi" % "poi" % "4.1.0" % "compile"

 // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
 libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided"
