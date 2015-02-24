package com.cj.kafka.rx

import MessageHelper._

class OffsetManager[T](
  commit: (OffsetManager[T], OffsetMap, CommitHook) => OffsetMap =
    (self: OffsetManager[T], offsets: OffsetMap, callback: CommitHook) => Map[TopicPartition, Long]()) {

  // Offset Manager keeps track of offsets per partition for a particular kafka stream

  private var currentOffsets = Map[TopicPartition, Long]()

  def check(message: Message[T]): Option[Message[T]] = {
    // updates internal offset state & determines whether the message is stale due to replay
    currentOffsets.get(message.topicPartition) match {
      case None => setOffsets(message)
      case Some(priorOffset) =>
        if (message.offset > priorOffset) setOffsets(message)
        else None
    }
  }

  def checkpoint(offsets: OffsetMap, callback: CommitHook) = commit(this, offsets, callback)

  private def setOffsets(message: Message[T]): Some[Message[T]] = {
    currentOffsets += message.topicPartition -> message.offset
    Some(message.copy(offsets = currentOffsets, callback = this.checkpoint))
  }

  def getOffsets: OffsetMap = currentOffsets

  def adjustOffsets(externalOffsets: OffsetMap): OffsetMap = adjustOffsets(externalOffsets, getOffsets)
  def adjustOffsets(externalOffsets: OffsetMap, internalOffsets: OffsetMap): OffsetMap = {
    // kafka rebalancing can cause skewed views of partition ownership
    // by reconciling our offsets with another view, we can determine which we have ownership over
    currentOffsets = currentOffsets.filter { case (topicPartition, offset) =>
      val externalOffset = externalOffsets.getOrElse(topicPartition, -1L)
      offset >= externalOffset
    }
    internalOffsets filter { case (topicPartition, offset) =>
      currentOffsets.contains(topicPartition)
    } map { case (topicPartition, offset) =>
      topicPartition -> (offset + 1)
    }
  }

}
