package net.manub.embeddedkafka

import org.scalatest.exceptions.TestFailedException

class KafkaSpec
  extends KafkaSpecSupport
    with EmbeddedKafka {

  "the withRunningKafka method" should {
    "start a Kafka broker on port 9092 by default" in {
      withRunningKafka {
        print("Hi")
        kafkaIsAvailable()
      }
    }

    "start a ZooKeeper instance on port 2181 by default" in {
      withRunningKafka {
        zookeeperIsAvailable()
      }
    }

    "stop Kafka and Zookeeper successfully" when {
      "the enclosed test passes" in {
        withRunningKafka {
          true shouldBe true
        }

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "the enclosed test fails" in {
        a[TestFailedException] shouldBe thrownBy {
          withRunningKafka {
            true shouldBe false
          }
        }

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }
    }

    "start a Kafka broker on a specified port" in {
      implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092)

      withRunningKafka {
        kafkaIsAvailable(9092)
      }
    }

    "start a Zookeeper server on a specified port" in {
      implicit val config = EmbeddedKafkaConfig(zooKeeperPort = 2181)

      withRunningKafka {
        zookeeperIsAvailable(2181)
      }
    }

  }
}

object SparkSpecObj extends KafkaSpec
{
  def main(args:Array[String])
  {
    val mr = new KafkaSpec()
  }
}
