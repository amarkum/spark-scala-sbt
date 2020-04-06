 name := "spark-demo"
 version := "1.0"
 scalaVersion := "2.12.3"

 /**
  * Add Dependency here.
  * libraryDependencies += "org.apache.poi" % "poi" % "4.1.0" % "compile"
  */

 // set the dependency tol "provided" when submitting JARs to cluster, and we dont want to create Fat JAR
 libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5" % "compile"
 libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "compile"