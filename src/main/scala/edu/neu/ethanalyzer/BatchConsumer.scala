package edu.neu.ethanalyzer

import edu.neu.ethanalyzer.JdbcConfig.{connectionProperties, jdbcUrl}
import edu.neu.ethanalyzer.Queries.{AVG_ETHER_PER_TRANS_QUERY, TOP_MINERS_QUERY}
import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/*
Loads data from CSV files, makes necessary aggregations and writes the dataframes to MYSQL tables
through JDBC
*/
object BatchConsumer {

  def write_to_db(df: DataFrame, table_name: String): Unit = {
    // Write it out to MYSQL
    df.write
      .mode("append")
      .jdbc(jdbcUrl, table_name, connectionProperties)
  }

  def execute_query(query: String, df: DataFrame, view_name: String, sc: SQLContext): DataFrame = {
    df.createOrReplaceTempView(view_name)
    sc.sql(query)
  }

  def main(args: Array[String]): Unit = {
    // Initiate Spark session
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sqlContext

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
    val avg_trans_val = execute_query(AVG_ETHER_PER_TRANS_QUERY,  transactions_data, "transactions", sc)

    write_to_db(avg_trans_val, "trans_val_batch")

    // Query to calculate top miners by reward
    val top_miners = execute_query(TOP_MINERS_QUERY, blocks_data, "blocks", sc)

    write_to_db(top_miners, "top_miners")

    spark.stop()
  }
}