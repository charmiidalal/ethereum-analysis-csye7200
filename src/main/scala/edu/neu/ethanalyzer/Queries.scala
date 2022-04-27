package edu.neu.ethanalyzer

object Queries {
  val AVG_ETHER_PER_TRANS_QUERY: String = """
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
  """

  val TOP_MINERS_QUERY =  """
      WITH mined_block AS (
        SELECT miner, DATE(timestamp)
        FROM blocks
        WHERE DATE(timestamp) > DATE_SUB(CURRENT_DATE(), 30)
        ORDER BY miner ASC)
      SELECT miner, COUNT(miner) AS total_block_reward
      FROM mined_block
      GROUP BY miner
      ORDER BY total_block_reward DESC
      LIMIT 10
    """
}