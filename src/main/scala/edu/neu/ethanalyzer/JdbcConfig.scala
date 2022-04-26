package edu.neu.ethanalyzer

object JdbcConfig {
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
}

