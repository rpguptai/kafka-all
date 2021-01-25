package com.versh.fundamental

import java.util.Properties
import com.typesafe.scalalogging.Logger
import io.confluent.kafka.serializers.{ AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericEnumSymbol, GenericRecord }


/*
*
* JSON
* {
    "fruit": "Apple",
    "size": "Large",
    "color": "Red"
}
*
* AVRO
*
* {
  "name": "Eatable",
  "type": "record",
  "namespace": "com.versh.fundamental",
  "fields": [
    {
      "name": "fruit",
      "type": "string"
    },
    {
      "name": "size",
      "type": "string"
    },
    {
      "name": "color",
      "type": "string"
    }
  ]
}
* */
object KafkaAvroProducer {

  case class Eatable(fruit: String,size: String,color: String)

  private[this] val logger = Logger(getClass.getSimpleName)
  private[this] val SCHEMA_REGISTRY_URL_VALUE = "http://localhost:8081"
  private[this] val BOOTSTRAP_SERVERS_VALUE = "localhost:19092,localhost:29092,localhost:39092"
  private[this] val TOPIC_NAME              = "test-text-topic"

  private[this] val SCHEMA_EATABLE_V1 = "/Eatable_v1.avsc"

  private[this] val schemaEatableV1 =
    new Schema.Parser().parse(getClass.getResourceAsStream(SCHEMA_EATABLE_V1))


  private[this] def newProducer(): KafkaProducer[String, GenericRecord] = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE)
    props.put(ACKS_CONFIG, "all")
    props.put(RETRIES_CONFIG, "0")
    props.put(BATCH_SIZE_CONFIG, "16384")
    props.put(LINGER_MS_CONFIG, "1")
    props.put(BUFFER_MEMORY_CONFIG, "33554432")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_VALUE)
    new KafkaProducer[String, GenericRecord](props)
  }

  def main(args: Array[String]): Unit = {
    logger.info(s"Start to produce on $TOPIC_NAME")
    val producer = newProducer()

    try {
      for (i <- 0 to 150) {
        val eatable = new GenericData.Record(schemaEatableV1)
        eatable.put("fruit","fruit"+i)
        eatable.put("size","size"+i)
        eatable.put("color","1234"+i)

        val record = new ProducerRecord[String, GenericRecord](TOPIC_NAME,i.toString+"HI",eatable)
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
