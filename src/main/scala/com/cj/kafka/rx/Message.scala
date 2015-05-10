package com.cj.kafka.rx

case class Message[K, V](
  key: K = null,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: OffsetMap = defaultOffsets,
  private[rx] val partialCommit: PartialCommit = defaultPartialCommit ) {

  val topicPartition = topic -> partition

  def commit(fn: OffsetMerge = (zkOffsets,proposedOffsets) => offsets): OffsetMap = {
    partialCommit(offsets, fn)
  }

  override def equals(other: Any) = {
    other match {
      case message: Message[K, V] =>
        message.key == key &&
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset
      case _ => false
    }
  }

  def produce[v](value: v): ProducedMessage[Null, v, K, V] = {
    produce[Null, v](null, value)
  }

  def produce[k, v](key: k, value: v): ProducedMessage[k, v, K, V] = {
    produce(key, value, null.asInstanceOf[Int])
  }

  def produce[k, v](key: k, value: v, partition: Int): ProducedMessage[k, v, K, V] = {
    new ProducedMessage(key = key, value = value, partition=partition, this)
  }

}
