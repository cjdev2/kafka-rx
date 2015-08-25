package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord

case class Record[K, V](
  key: K = null,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  private[rx] override val commitfn: Commit = defaultCommit)
  extends Committable[V] {

  private[rx] def this(msg: MessageAndMetadata[K, V], commit: Commit) = this(msg.key(), msg.message(), msg.topic, msg.partition, msg.offset, commit)
  private[rx] def this(msg: MessageAndMetadata[K, V]) = this(msg, defaultCommit)

  val topicPartition = topic -> partition

  def commit(merge: OffsetMerge): OffsetMap = commitfn(merge)

  override def equals(other: Any) = {
    other match {
      case message: Record[K, V] =>
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset
      case _ => false
    }
  }

  def produce[k, v](topic: String, key: k, value: v): Committable[ProducerRecord[k, v]] = {
    derive(new ProducerRecord[k, v](topic, key, value))
  }

  def produce(topic: String): Committable[ProducerRecord[K, V]] = {
    derive(new ProducerRecord[K, V](topic, partition, key, value))
  }

  def produce[k, v](topic: String, partition: Int, key: k, value: v): Committable[ProducerRecord[k, v]] = {
    derive(new ProducerRecord[k, v](topic, partition, key, value))
  }


}
