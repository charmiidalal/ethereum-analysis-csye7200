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
Running this program simulates an environment where a number of transactions are occurring every
N seconds.
*/

object RowWiseProducer {
  // Function to execute two tasks in parallel
  def parallel[A, B](taskA: =>A, taskB: =>B): (A,B) = {
    val fB:Future[B] = Future { taskB }
    val a:A = taskA
    val b:B = Await.result(fB, Duration.Inf)
    (a,b)
  }

  // Define properties for Kafka Producer
  val props = new Properties()
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")

  val producer = new KafkaProducer[String, String](props)


  /*
    Writes the data to Kafka topic with the specific batch size, then sleeps
    for a specific and writes it again
  */
  def write_to_topic(data: Array[Row], topic: String, batch_size: Int, sleep_time: Int): Unit = {

    var count = 0

    for (row <- data) {
      val key = row.get(0).toString

      val value = row.get(1).toString

      val record = new ProducerRecord[String, String](topic, key, value)
      producer.send(record)

      println(s"Wrote to topic: ${topic}")

      count = count + 1

      if (count % batch_size == 0) {println(s"Sleeping for ${sleep_time} seconds"); Thread.sleep(sleep_time * 1000)}
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
      write_to_topic(trans_query, "eth_transactions", 50, 5),
      write_to_topic(blocks_query, "eth_blocks", 50, 5)
    )

  }
}
