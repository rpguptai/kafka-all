package com.versh.fundamental

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import java.time.Duration
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.Logger
import scala.collection.JavaConverters.asJavaCollectionConverter
import org.apache.kafka.clients.consumer.ConsumerConfig.{ BOOTSTRAP_SERVERS_CONFIG, _ }
import org.apache.kafka.common.serialization.StringDeserializer


object KafkaConsumerBasic {
  private[this] val logger = Logger(getClass.getSimpleName)

  /*
  * run docker-compose up -d
  * and once cluster is up and running
  * create a topic with name test-text-topic
  * with replication factor 3 and and partition
  * count 3
  *
  * */
  private[this] val BOOTSTRAP_SERVERS_VALUE = "localhost:19092,localhost:29092,localhost:39092"
  private[this] val TOPIC_NAME              = "test-text-topic"
  private[this] val GROUP_ID_VALUE          = "consumer-2"
  private[this] val TIMEOUT_MILLIS          = 100

  private[this] def newConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE)
    props.put(GROUP_ID_CONFIG, GROUP_ID_VALUE)
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "100")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    new KafkaConsumer(props)
  }

  def main(args: Array[String]): Unit =
    consume(newConsumer(), TOPIC_NAME, TIMEOUT_MILLIS)


  def consume[K, V](consumer: KafkaConsumer[K, V], topic: String, timeoutMillis: Long): Unit = {
    printf(s"Start to consume from $topic")
    consumer.subscribe(List(topic).asJavaCollection)

    Try {
      while (true) {
        val records: ConsumerRecords[K, V] = consumer.poll(Duration.ofMillis(timeoutMillis))
        records.iterator().forEachRemaining { record: ConsumerRecord[K, V] =>
          printf(s"""
                         |message
                         |  offset=${record.offset}
                         |  partition=${record.partition}
                         |  key=${record.key}
                         |  value=${record.value}
           """.stripMargin)
        }
      }
    } match {
      case Success(_) =>
        printf(s"Finish to consume from $topic")
      case Failure(exception) =>
        printf(s"Finish to consume from $topic with error", exception)
    }

    consumer.close()
  }

}
