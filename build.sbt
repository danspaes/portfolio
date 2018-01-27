name := "challenge"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

