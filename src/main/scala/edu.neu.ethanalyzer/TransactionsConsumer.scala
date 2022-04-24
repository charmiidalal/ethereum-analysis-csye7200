package edu.neu.ethanalyzer

import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StringType


/*
Reads the datastream from Kafka topics through the Spark-Streaming API, and writes it
to MySQL database in realtime using JDBC connection string.
*/
object TransactionsConsumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val trans_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_transactions")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), transactions_schema).alias("transaction"))

    trans_data.printSchema()

    val blocks_data = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "eth_blocks")
      .load()
      .select(col("value").cast(StringType).as("col"))
      .select(from_json(col("col"), blocks_schema).alias("block"))

    blocks_data.printSchema()

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

    val query1 = trans_data
      .select("transaction.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "transactions", connectionProperties)
      }})
      .start()

    val query2 = blocks_data
      .select("block.*")
      .writeStream
      .trigger(Trigger.ProcessingTime(2000))
      .foreachBatch({ (batchDF: Dataset[Row], _: Long) => {
        batchDF.write.mode("append")
          .jdbc(jdbcUrl, "blocks", connectionProperties)
      }})
      .start()

    query1.awaitTermination()
    query2.awaitTermination()
    spark.stop()
  }
}