package edu.neu.ethanalyzer

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}


object Schemas {
  val transactions_schema: StructType = StructType(Array(
    StructField("hash", StringType, nullable = false),
    StructField("nonce", IntegerType, nullable = false),
    StructField("transaction_index", IntegerType, nullable = false),
    StructField("from_address", StringType, nullable = false),
    StructField("to_address", StringType),
    StructField("value", DoubleType),
    StructField("gas", IntegerType),
    StructField("gas_price", IntegerType),
    StructField("input", StringType),
    StructField("receipt_cumulative_gas_used", IntegerType),
    StructField("receipt_gas_used", IntegerType),
    StructField("receipt_contract_address", StringType),
    StructField("receipt_root", StringType),
    StructField("receipt_status", IntegerType),
    StructField("block_timestamp", TimestampType, nullable = false),
    StructField("block_number", IntegerType, nullable = false),
    StructField("block_hash", StringType, nullable = false)
  ))

  val blocks_schema: StructType = StructType(Array(
    StructField("timestamp", TimestampType, nullable = false),
    StructField("number", IntegerType, nullable = false),
    StructField("hash", StringType, nullable = false),
    StructField("parent_hash", StringType),
    StructField("nonce", StringType, nullable = false),
    StructField("sha3_uncles", StringType),
    StructField("logs_bloom", StringType),
    StructField("transactions_root", StringType),
    StructField("state_root", StringType),
    StructField("receipts_root", StringType),
    StructField("miner", StringType),
    StructField("difficulty", IntegerType),
    StructField("total_difficulty", IntegerType),
    StructField("size", IntegerType),
    StructField("extra_data", StringType),
    StructField("gas_limit", IntegerType),
    StructField("gas_used", IntegerType),
    StructField("transaction_count", IntegerType),
    StructField("base_fee_per_gas", IntegerType)
  ))
}

