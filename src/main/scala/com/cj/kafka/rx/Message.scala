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

  def commit(fn: OffsetMerge = (_,_) => offsets): OffsetMap = {
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

  def produce[_V](value: _V): ProducerMessage[Null, _V] = {
    produce[Null, _V](null, value)
  }

  def produce[_K, _V](key: _K, value: _V): ProducerMessage[_K, _V] = {
    produce(key, value, null.asInstanceOf[Int])
  }

  def produce[_K, _V](key: _K, value: _V, partition: Int): ProducerMessage[_K, _V] = {
    ProducerMessage(key = key, value = value, partition=partition, sourceMessage = Some(this))
  }

}
