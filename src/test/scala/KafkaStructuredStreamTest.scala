package org.sparkstream.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSpec

trait SparkSessionWrapper{
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("StructuredNetworkWordCount")
    .getOrCreate()
}

class KafkaStructuredStreamTest extends FunSpec with DataFrameComparer with SparkSessionWrapper {

  it("counts the occurrences of each words in the stream") {

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val sqlCtx = spark.sqlContext

    import spark.implicits._

    val events = MemoryStream[String]
    val sessions = events.toDF()
    assert(sessions.isStreaming, "sessions must be a streaming Dataset")

    val transformedSessions = KafkaStructuredStream.countWords(sessions)

    val streamingQuery = transformedSessions
      .writeStream
      .format("memory")
      .queryName("queryName")
      .outputMode("complete")
      .start

    val batch = Seq("hello world", "hello world")

    val currentOffset = events.addData(batch)

    streamingQuery.processAllAvailable()

    events.commit(currentOffset.asInstanceOf[LongOffset])

    val res = spark.sql("select * from queryName")


    val expectedSchema = List(
      StructField("value", StringType, true),
      StructField("count", LongType, false)
    )

    val expectedData = Seq(
      Row("hello", 2.toLong),
      Row("world", 2.toLong)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(res, expectedDF)

  }

}