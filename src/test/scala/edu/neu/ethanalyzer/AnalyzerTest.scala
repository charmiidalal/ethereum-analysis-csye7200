package edu.neu.ethanalyzer


import edu.neu.ethanalyzer.BatchConsumer.execute_query
import edu.neu.ethanalyzer.Queries.{AVG_ETHER_PER_TRANS_QUERY, TOP_MINERS_QUERY}
import edu.neu.ethanalyzer.Schemas.{blocks_schema, transactions_schema}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class AnalyzerTest extends AnyFlatSpec with Matchers {

  implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("EthereumAnalytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


  val sc: SQLContext = spark.sqlContext

  import spark.implicits._

  behavior of "execute_query"

  it should "successfully return the expected df for the AVG_ETHER_PER_TRANS_QUERY on the dummy transactions df" in {
    val trans: DataFrame = spark.read.schema(transactions_schema).csv("src/test/resources/transactions.csv")
    val actual_result: DataFrame = execute_query(AVG_ETHER_PER_TRANS_QUERY, trans, "transactions", sc)

    val expected_result = Seq((1.0E-15, 7.418689999999999E-4, "2020-03-19")).toDF("sum_tx_ether", "avg_gas_cost", "tx_date")

    actual_result.show()
    val diff = actual_result.except(expected_result)

    assert(diff.count() == 0)
  }

  it should "successfully return the expected df for the TOP_MINERS_QUERY on the dummy blocks df" in {
    val blocks: DataFrame = spark.read.schema(blocks_schema).csv("src/test/resources/blocks.csv")
    val actual_result: DataFrame = execute_query(TOP_MINERS_QUERY, blocks, "blocks", sc)

    val expected_result = Seq(("0x2a20380dca5bc24d052acfbf79ba23e988ad0050",5),
      ("0xc365c3315cf926351ccaf13fa7d19c8c4058c8e1",4),
      ("0x03e75d7dd38cce2e20ffee35ec914c57780a8e29",3),
      ("0x28846f1ec065eea239152213373bb58b1c9fc93b",3),
    ("0xc730b028da66ebb14f20e67c68dd809fbc49890d",3),
    ("0xab3b229eb4bcff881275e7ea2f0fd24eeac8c83a",2),
    ("0x8f03f1a3f10c05e7cccf75c1fd10168e06659be7",2),
    ("0x646db8ffc21e7ddc2b6327448dd9fa560df41087",2),
    ("0xb645001fc19bafec83c1ef2cb3bc3516c7e0916c",1),
    ("0x433022c4066558e7a32d850f02d2da5ca782174d",1),
    ).toDF("miner", "total_block_reward")

    val diff = actual_result.except(expected_result)

    assert(diff.count() == 0)
  }
}