package org.sparkstream.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaStructuredStream{


  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()


  import spark.implicits._
  def countWords(df:DataFrame):DataFrame={
    val lines = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]

    val words = lines.map(x => x).as[String].flatMap(_.split(" "))

    val result = words.groupBy("value").count()

     result
  }

  def main(args:Array[String]){

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9092")
      .option("subscribe", "testtopic")
      .load()

    val query = countWords(df)

    query.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()


    spark.stop()
  }
}