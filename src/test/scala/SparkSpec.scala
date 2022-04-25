import org.apache.spark.sql.{DataFrame, SparkSession, functions}


class SparkSpec  {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("EthereumAnaysis")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val transactionMetadata: DataFrame = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/test/resources/transaction.csv")

  def meanEtherValuebyTransaction() : DataFrame = {
    val transaction_analysis: DataFrame = transactionMetadata.select("from", "value").groupBy("from").agg(functions.avg("value"))
    println(s"Mean of Ether value by Transaction")
    println(s"*************************************")
    for (transaction_mean <- transaction_analysis.collect()) {
      println(s"Mean of ether of ${transaction_mean(0)} -  ${transaction_mean(1)}")
    }
    transaction_analysis
  }

}

object SparkSpecObj extends SparkSpec
{
  def main(args:Array[String])
  {
    val mr = new SparkSpec()
    mr.meanEtherValuebyTransaction()
  }
}
