package com.cj.kafka.rx


// alternate version of kafkas MessageAndMetadata class that tracks consumer offsets per message
case class Message[T](
  value: T,
  topic: String,
  partition: Int,
  offset: Long,
  offsets: Map[Int, Long] = Map[Int, Long](),
  callback: (Map[Int, Long], Map[Int, Long] => Unit) => Map[Int, Long] = { case (x, fn) =>
    fn(x); x
  }
) {

  def checkpoint(hook: (Map[Int, Long]) => Unit = { _ => () }): Map[Int, Long] = {
    callback(offsets, hook)
  }

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