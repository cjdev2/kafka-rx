package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord

// alternate version of kafkas MessageAndMetadata class that tracks consumer offsets per message
case class Message[T](
  value: T,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: OffsetMap = Map[TopicPartition, Long](),
  commitWith: PartialCommit = { case (x, fn) => fn(x,x); x } ) {

  val topicPartition = topic -> partition

  def commit(fn: OffsetMerge = (_,_) => offsets): OffsetMap = commitWith(offsets, fn)

  override def equals(other: Any) = {
    other match {
      case message: Message[T] =>
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset
      case _ => false
    }
  }

  def kafkaMessage: MessageAndMetadata[Array[Byte], Array[Byte]] = {
    val message = new kafka.message.Message(value.asInstanceOf[Array[Byte]])
    val decoder = new kafka.serializer.DefaultDecoder
    MessageAndMetadata(topic, partition, message, offset, decoder, decoder)
  }
  def newCommittableProducerMessage[K, V](topic: String, key: K, value: V, partition: Int) = ProducerMessage[K, V](topic, key, value, partition, Some(commit _))
  def newCommittableProducerMessage[K, V](topic: String, key: K, value: V) = ProducerMessage[K, V](topic, key, value, Some(commit _))
  def newCommittableProducerMessage[V](topic: String, value: V) = ProducerMessage[Array[Byte], V](topic, value, Some(commit _))
}

case class ProducerMessage[K, V](topic: String, key: K, value: V, partition: Int, commitWith: Option[OffsetMerge => OffsetMap]) {
  def commit() = commitWith match  {
    case Some(fn) => fn
    case None => Map[TopicPartition, Long]()
  }
  def producerRecord: ProducerRecord[K, V] = new ProducerRecord[K, V](topic, partition, key, value)
}

object ProducerMessage {
  def apply[K, V](topic: String, key: K, value: V, commitWith: Option[OffsetMerge => OffsetMap] = None) = new ProducerMessage[K, V](topic, key, value, null.asInstanceOf[Int], commitWith)
  def apply[K, V](topic: String, value: V, commitWith: Option[OffsetMerge => OffsetMap]) = new ProducerMessage(topic, null.asInstanceOf[K], value, null.asInstanceOf[Int], commitWith)
  def apply[K, V](topic: String, value: V) = new ProducerMessage(topic, null.asInstanceOf[K], value, null.asInstanceOf[Int], None)

}
