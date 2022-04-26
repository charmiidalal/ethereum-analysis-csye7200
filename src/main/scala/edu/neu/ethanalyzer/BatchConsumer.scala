package edu.neu.ethanalyzer

import edu.neu.ethanalyzer.JdbcConfig.{connectionProperties, jdbcUrl}
import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.SparkSession

/*
Loads data from CSV files, makes necessary aggregations and writes the dataframes to MYSQL tables
through JDBC
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

    // Read Transactions and Blocks CSV files
    val transactions_data = spark.read.schema(transactions_schema).csv("data/transactions/eth_transactions.csv")
    val blocks_data = spark.read.schema(blocks_schema).csv("data/blocks/blocks.csv")

    transactions_data.printSchema()
    blocks_data.printSchema()

    transactions_data.write.mode("append").jdbc(jdbcUrl, "transactions", connectionProperties)
    blocks_data.write.mode("append").jdbc(jdbcUrl, "blocks", connectionProperties)

    // Create temp views to enable performing SQL queries
    transactions_data.createOrReplaceTempView("transactions")
    blocks_data.createOrReplaceTempView("blocks")

    // Query to calculate the average ether cost per transaction
    val avg_trans_val = spark.sqlContext.sql("""
      SELECT
        SUM(value/POWER(10,18)) AS sum_tx_ether,
        AVG(gas_price*(receipt_gas_used/POWER(10,18))) AS avg_tx_gas_cost,
        DATE(block_timestamp) AS tx_date
      FROM
        transactions
      WHERE TRUE
      AND receipt_status = 1
      AND value > 0
      GROUP BY tx_date
      ORDER BY tx_date
    """)

    // Write it out to MYSQL
    avg_trans_val
      .write
      .mode("append")
      .jdbc(jdbcUrl, "trans_val_batch", connectionProperties)

    spark.stop()
  }
}