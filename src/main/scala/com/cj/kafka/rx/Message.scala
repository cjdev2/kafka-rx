package com.cj.kafka.rx

case class Message[K, V](
  key: K = null,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  private[rx] val commitWith: PartialCommit = defaultCommit) {

  val topicPartition = topic -> partition

  def commit(merge: OffsetMerge = defaultMerge): OffsetMap = {
    commitWith(merge)
  }

  override def equals(other: Any) = {
    other match {
      case message: Message[K, V] =>
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset
      case _ => false
    }
  }

  def produce: ProducedMessage[K, V] = {
    produce[K, V](key, value, partition)
  }

  def produce[v](value: v): ProducedMessage[Null, v] = {
    produce[Null, v](null, value)
  }

  def produce[k, v](key: k, value: v): ProducedMessage[k, v] = {
    new ProducedMessage(key, value, sourceMessage = this)
  }

  def produce[k, v](key: k, value: v, partition: Int): ProducedMessage[k, v] = {
    new ProducedMessage(key = key, value = value, partition=partition, this)
  }

}
