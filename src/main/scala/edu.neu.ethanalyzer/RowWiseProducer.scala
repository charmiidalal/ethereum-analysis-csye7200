package edu.neu.ethanalyzer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import Schemas.{blocks_schema, transactions_schema}
import scala.concurrent.{Await,Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/*
Reads the data from .csv files and writes it to corresponding Kafka topics in batches.
Running this program simulates an enviornment where a number of transactions are occuring every
N seconds.
*/
object RowWiseProducer {
/*
Checks the source file(csv) for addition of new rows
*/
  def parallel[A, B](taskA: =>A, taskB: =>B): (A,B) = {
    val fB:Future[B] = Future { taskB }
    val a:A = taskA
    val b:B = Await.result(fB, Duration.Inf)
    (a,b)
  }
/*
Each topic contains rows divided and read in batches every 5 secs
*/
  def write_to_topic(data: Array[Row], topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    val producer = new KafkaProducer[String, String](props)

    var count = 0

    for (row <- data) {
      val key = row.get(0).toString

      val value = row.get(1).toString

      val record = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(record)

      println(s"Wrote to topic: ${topic}")

      count = count + 1

      if (count % 50 == 0) {println("Sleeping for 5 seconds"); Thread.sleep(5000)}
    }
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    val spc: SparkContext = spark.sparkContext
    val sc: SQLContext= spark.sqlContext
    spark.sparkContext.setLogLevel("ERROR")


    val transactions_data = spark.read.schema(transactions_schema).csv("data/transactions/*.csv")
    val blocks_data = spark.read.schema(blocks_schema).csv("data/blocks/*.csv")

    val trans_query = transactions_data.selectExpr("CAST(hash AS STRING) AS key", "to_json(struct(*)) AS value").collect()
    val blocks_query = blocks_data.selectExpr("CAST(hash AS STRING) AS key", "to_json(struct(*)) AS value").collect()


    parallel(
      write_to_topic(trans_query, "eth_transactions"),
      write_to_topic(blocks_query, "eth_blocks")
    )

  }
}
