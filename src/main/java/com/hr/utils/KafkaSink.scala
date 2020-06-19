package com.hr.utils

/**
  * HF
  * 2020-06-05 11:54
  */
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


//用来广播kafka生产者的
class KafkaSink[K,V](createProducer:()=> KafkaProducer[K,V]) extends Serializable{

  lazy val producer= createProducer()
  def send(topic:String, key:K, value:V):Future[RecordMetadata] =
    producer.send(new ProducerRecord[K,V](topic,key,value))

  def send(topic:String,value:V):Future[RecordMetadata] =
    producer.send(new ProducerRecord[K,V](topic,value))
}
object KafkaSink {
  import scala.collection.JavaConversions._

  def apply[K,V](config: Map[String,Object]):KafkaSink[K,V]= {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K,V](config)
      sys.addShutdownHook{
        producer.close()
      }
      producer
    }

    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K,V] = apply(config.toMap)
}
