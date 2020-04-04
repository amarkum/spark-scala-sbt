 name := "spark-demo"
 version := "1.0"
 scalaVersion := "2.12.3"

 // https://mvnrepository.com/artifact/org.apache.poi/poi
 libraryDependencies += "org.apache.poi" % "poi" % "4.1.0" % "compile"

 // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
 libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

 // https://mvnrepository.com/artifact/org.apache.spark/spark-core
 libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
