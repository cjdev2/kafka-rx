package com.cj.kafka.rx

import org.apache.kafka.clients.producer.ProducerRecord

case class ProducerMessage[K, V] (
  key: K,
  value: V,
  partition: Int,
  sourceMessage: Option[Message[_, _]] = None) {

  def toProducerRecord(topic: String) = {
    new ProducerRecord[K, V](topic, partition, key, value)
  }

}
