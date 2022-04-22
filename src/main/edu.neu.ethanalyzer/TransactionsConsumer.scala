package edu.neu.ethanalyzer

import org.apache.spark.sql.{Dataset, Row, SparkSession}



object TransactionsConsumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.stop()
  }
}