package edu.neu.ethanalyzer

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import Schemas.{blocks_schema, transactions_schema}
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

  def parallel[A, B](taskA: =>A, taskB: =>B): (A,B) = {
    val fB:Future[B] = Future { taskB }
    val a:A = taskA
    val b:B = Await.result(fB, Duration.Inf)
    (a,b)
  }

  def write_to_topic(data: Array[Row], topic: String): Unit = ???

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
