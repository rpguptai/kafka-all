package com.versh.fundamental

import java.util.Properties
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.kafka.common.serialization.StringSerializer

/*
  * run docker-compose up -d
  * and once cluster is up and running
  * create a topic with name test-text-topic
  * with replication factor 3 and and partition
  * count 3
  *
  * */

object KafkaProducer {

  private[this] val logger = Logger(getClass.getSimpleName)

  private[this] val BOOTSTRAP_SERVERS_VALUE = "localhost:19092,localhost:29092,localhost:39092"
  private[this] val TOPIC_NAME              = "test-text-topic"

  private[this] def newProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE)
    props.put(ACKS_CONFIG, "all")
    props.put(RETRIES_CONFIG, "0")
    props.put(BATCH_SIZE_CONFIG, "16384")
    props.put(LINGER_MS_CONFIG, "1")
    props.put(BUFFER_MEMORY_CONFIG, "33554432")
    props.put(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    new KafkaProducer(props)
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Start to produce on $TOPIC_NAME")
    val producer = newProducer()

    try {
      for (i <- 0 to 150) {
        val record = new ProducerRecord[String, String](TOPIC_NAME,i.toString, "This is Just a test string number " + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
    logger.info(s"Finish to produce on $TOPIC_NAME")
  }

}


