package edu.neu.ethanalyzer

import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StringType


/*
Reads the datastream from Kafka topics through the Spark-Streaming API, and writes it
to MySQL database in realtime using JDBC connection string.
*/
object BatchConsumer {

  def main(args: Array[String]): Unit = {
    // Initiate Spark session
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val transactions_data = spark.read.schema(transactions_schema).csv("data/transactions/*.csv")
    val blocks_data = spark.read.schema(blocks_schema).csv("data/blocks/*.csv")

    transactions_data.createOrReplaceTempView("transactions")
    blocks_data.createOrReplaceTempView("blocks")


    // Make connection to mysql database to dump the data
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "crypto_db"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.
    import java.util.Properties
    val connectionProperties = new Properties()

    connectionProperties.put("user", "root")
    connectionProperties.put("password", "csye7200")

    val driverClass = "com.mysql.jdbc.Driver"
    connectionProperties.setProperty("Driver", driverClass)

    val avg_trans_val = spark.sqlContext.sql("""
      SELECT
        SUM(value/POWER(10,18)) AS sum_tx_ether,
        AVG(gas_price*(receipt_gas_used/POWER(10,18))) AS avg_tx_gas_cost,
        DATE(timestamp) AS tx_date
      FROM
        transactions,
        blocks
      WHERE TRUE
      AND transactions.block_number = blocks.number
      AND receipt_status = 1
      AND value > 0
      GROUP BY tx_date
      ORDER BY tx_date
    """).write
      .mode("append")
      .jdbc(jdbcUrl, "trans_val_batch", connectionProperties)


    println("Wrote to DB")

    spark.stop()
  }
}