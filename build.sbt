scalaVersion := "2.12.15"

name := "EthereumAnalytics"
version := "1.0"

val sparkVersion = "3.2.1"
val kafkaVersion = "3.1.0"
val akkaVersion = "2.5.14"

libraryDependencies ++= Seq(
"org.scalatest" %% "scalatest" % "2.12.15" % Test,
"org.mockito" %% "mockito-scala" % "1.16.23" % Test)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Test,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.2",
  "mysql" % "mysql-connector-java" % "8.0.28",
  "net.manub" %% "scalatest-embedded-kafka" % "0.14.0" % "test"
)

ThisBuild / assemblyMergeStrategy  := {
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" => MergeStrategy.first
  case x if x.contains("io.netty.versions.properties") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Logger.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "nowarn$.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "nowarn.class" => MergeStrategy.first
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

