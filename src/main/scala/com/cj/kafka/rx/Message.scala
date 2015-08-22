package com.cj.kafka.rx

import org.apache.kafka.clients.producer.ProducerRecord

case class Message[K, V](
  key: K = null,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  private[rx] override val commitfn: Commit = defaultCommit)
  extends Committable[V] {

  val topicPartition = topic -> partition

  def commit(merge: OffsetMerge): OffsetMap = commitfn(merge)

  override def equals(other: Any) = {
    other match {
      case message: Message[K, V] =>
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset
      case _ => false
    }
  }

  def produce(topic: String): Committable[ProducerRecord[K, V]] = {
    derive(new ProducerRecord[K, V](topic, partition, key, value))
  }

  def produce[v](topic: String, value: v): Committable[ProducerRecord[Null, v]] = {
    produce[Null, v](topic, null, value)
  }

  def produce[k, v](topic: String, key: k, value: v): Committable[ProducerRecord[k, v]] = {
    derive(new ProducerRecord[k, v](topic, key, value))
  }

  def produce[k, v](topic: String, partition: Int, key: k, value: v): Committable[ProducerRecord[k, v]] = {
    derive(new ProducerRecord[k, v](topic, partition, key, value))
  }


}
