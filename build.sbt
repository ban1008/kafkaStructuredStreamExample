name := "KafkaStructuredStreamExample"

version := "0.1"

idePackagePrefix := Some("org.sparkstream.example")

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
)

