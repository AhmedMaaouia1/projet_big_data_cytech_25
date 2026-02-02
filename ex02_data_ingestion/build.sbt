name := "ex02-data-ingestion"

version := "0.1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
)
