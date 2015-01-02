package com.cj.kafka.rx

case class Message[T](
  value: T,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: Map[Int, Long] = Map[Int, Long](),
  private val checkpointFn: (Map[Int, Long]) => Map[Int, Long] = _ => Map[Int, Long]()
) {
  // alternate version of kafkas MessageAndMetadata class that tracks consumer offsets per message
  def checkpoint() = checkpointFn(offsets)

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