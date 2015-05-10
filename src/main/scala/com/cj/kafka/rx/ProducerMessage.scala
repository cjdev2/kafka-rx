package com.cj.kafka.rx

import org.apache.kafka.clients.producer.ProducerRecord

case class ProducerMessage[K, V](key: K, value: V, partition: Int = Int.MaxValue) {

  def toProducerRecord(topic: String) = {
    if (partition == Int.MaxValue) {
      new ProducerRecord[K, V](topic, key, value)
    } else {
      new ProducerRecord[K, V](topic, partition, key, value)
    }
  }

}

class ProducedMessage[K, V, k, v](key: K, value: V, partition: Int, val sourceMessage: Message[k, v])
  extends ProducerMessage(key, value, partition) {
}