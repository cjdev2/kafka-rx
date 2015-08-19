package com.cj.kafka.rx

case class Message[K, V](
  key: K = null,
  value: V,
  topic: String,
  partition: Int,
  offset: Long,
  private[rx] val commitfn: Commit = defaultCommit)
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

  def produce(topic: String): ProducedMessage[K, V] = {
    produce[K, V](topic, partition, key, value)
  }

  def produce[v](topic: String, value: v): ProducedMessage[Null, v] = {
    produce[Null, v](topic, null, value)
  }

  def produce[k, v](topic: String, key: k, value: v): ProducedMessage[k, v] = {
    new ProducedMessage[k, v](this, topic, key, value)
  }

  def produce[k, v](topic: String, partition: Int, key: k, value: v): ProducedMessage[k, v] = {
    new ProducedMessage[k, v](this, topic, partition, key, value)
  }

  def map[R](fn: (V)=>R): Message[K,R] = {
    new Message(key, fn(value), topic, partition, offset, commitfn)
  }

}
