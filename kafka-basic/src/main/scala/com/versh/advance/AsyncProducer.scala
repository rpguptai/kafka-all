package com.versh.advance

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.ProducerConfig.{ACKS_CONFIG, BATCH_SIZE_CONFIG, BOOTSTRAP_SERVERS_CONFIG, BUFFER_MEMORY_CONFIG, KEY_SERIALIZER_CLASS_CONFIG, LINGER_MS_CONFIG, RETRIES_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG}
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.Promise
import org.apache.kafka.clients.producer.{Callback, RecordMetadata, ProducerRecord, KafkaProducer}



import java.util.Properties

object AsyncProducer {

  private[this] val logger = Logger(getClass.getSimpleName)

  private[this] val BOOTSTRAP_SERVERS_VALUE = "localhost:19092,localhost:29092,localhost:39092"
  private[this] val TOPIC_NAME              = "test-text-topic"

  private[this] def newProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE)
    props.put(ACKS_CONFIG, "all")
    props.put(RETRIES_CONFIG, "3")
    props.put(BATCH_SIZE_CONFIG, "16384")
    props.put(LINGER_MS_CONFIG, "5")
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
        sendAsync(producer,TOPIC_NAME,i.toString, "This is Just a test string number " +i)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
    logger.info(s"Finish to produce on $TOPIC_NAME")
  }


  def sendSync(producer: KafkaProducer[String, String] ,topic: String,key: String, value: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, value)
    val metadata= producer.send(record).get()
    printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
      record.key(), record.value(),
      metadata.partition(),
      metadata.offset())
  }

  def sendAsync(producer: KafkaProducer[String, String] ,topic: String,key: String, value: String):Unit = {
    val record = new ProducerRecord[String, String](topic,key, value)
    val p = Promise[(RecordMetadata, Exception)]()
    val metadata = producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        p.success((metadata, exception))
      }
    })
    printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n",
      record.key(), record.value(),
      metadata.get().partition(),
      metadata.get().offset())
  }
}


