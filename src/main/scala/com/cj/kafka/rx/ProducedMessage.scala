package com.cj.kafka.rx

import org.apache.kafka.clients.producer.ProducerRecord

class ProducedMessage[K, V](val origin: Message[_, _], val record: ProducerRecord[K, V])
  extends Message[K, V](record.key, record.value, record.topic, -1, -1, origin.commitfn) {

  private[rx] def this(message: Message[_, _], topic: String, value: V) =
    this(message, new ProducerRecord[K, V](topic, value))
  private[rx] def this(message: Message[_, _], topic: String, key: K, value: V) =
    this(message, new ProducerRecord[K, V](topic, key, value))
  private[rx] def this(message: Message[_, _], topic: String, partition: Integer, key: K, value: V) =
    this(message, new ProducerRecord[K, V](topic, partition, key, value))

}