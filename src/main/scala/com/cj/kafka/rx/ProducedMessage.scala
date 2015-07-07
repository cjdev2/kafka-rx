package com.cj.kafka.rx

import org.apache.kafka.clients.producer.ProducerRecord

class ProducedMessage[K, V](key: K, value: V, partition: Integer = null, val sourceMessage: Message[_, _]) {

  def toProducerRecord(topic: String) = {
    new ProducerRecord[K, V](topic, partition, key, value)
  }

}