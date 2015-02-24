package com.cj.kafka.rx

object MessageHelper {
  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type CommitHook = OffsetMap => Unit
}
import MessageHelper._

// alternate version of kafkas MessageAndMetadata class that tracks consumer offsets per message
case class Message[T](
  value: T,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: OffsetMap = Map[TopicPartition, Long](),
  callback: (OffsetMap,CommitHook) => OffsetMap = { case (x, fn) =>
    fn(x); x
  }
) {

  def checkpoint(): OffsetMap = {
    checkpoint(_ => ())
  }

  def checkpoint(hook: CommitHook): OffsetMap = {
    callback(offsets, hook)
  }

  val topicPartition = topic -> partition

  override def equals(other: Any) = {
    other match {
      case message: Message[T] =>
        message.offsets == offsets &&
        message.topic == topic &&
        message.partition == partition &&
        message.offset == offset &&
        message.value == value
      case _ => false
    }
  }
}