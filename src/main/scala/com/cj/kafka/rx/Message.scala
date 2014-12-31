package com.cj.kafka.rx

import kafka.message.MessageAndMetadata

case class Message[T](
  value: T,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: Map[Int, Long] = Map[Int, Long](),
  private val checkpointFn: (Map[Int, Long]) => Map[Int, Long] = _ => Map[Int, Long]()
) {
  def checkpoint() = checkpointFn(offsets)

  override def equals(other: Any) = {
    other match {
      case message: Message[T] =>
        message.offsets == offsets &&
        message.topic == topic &&
        message.partition == partition
        message.offset == offset &&
        message.value == value
      case _ => false
    }
  }
}

object Message {
  def fromKafka(message: MessageAndMetadata[Array[Byte], Array[Byte]]): Message[Array[Byte]] = {
    Message(value=message.message(), topic=message.topic, partition=message.partition, offset=message.offset)
  }
}