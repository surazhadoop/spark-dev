name := "SparkSQLCode"
version := "0.1"
scalaVersion := "2.13.9"
val sparkVersion= "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion