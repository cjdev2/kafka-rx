package com.cj.kafka.rx

class OffsetManager[T](
  commit: (OffsetManager[T], Map[Int, Long], (Map[Int, Long]) => Unit) => Map[Int, Long] =
    (self: OffsetManager[T], offsets: Map[Int, Long], callback: (Map[Int, Long]) => Unit) => Map[Int, Long]()) {

  // Offset Manager keeps track of offsets per partition for a particular kafka stream
  type PartitionOffsets = Map[Int, Long]
  type CommitHook = (Map[Int, Long]) => Unit

  private var currentOffsets = Map[Int, Long]()

  def check(message: Message[T]): Option[Message[T]] = {
    // updates internal offset state & determines whether the message is stale due to replay
    currentOffsets.get(message.partition) match {
      case None => setOffsets(message)
      case Some(priorOffset) =>
        if (message.offset > priorOffset) setOffsets(message)
        else None
    }
  }

  def checkpoint(offsets: PartitionOffsets, callback: CommitHook) = commit(this, offsets, callback)

  private def setOffsets(message: Message[T]): Some[Message[T]] = {
    currentOffsets += message.partition -> message.offset
    Some(message.copy(offsets = currentOffsets, callback = this.checkpoint))
  }

  def getOffsets: PartitionOffsets = currentOffsets

  def adjustOffsets(externalOffsets: PartitionOffsets): PartitionOffsets = adjustOffsets(externalOffsets, getOffsets)
  def adjustOffsets(externalOffsets: PartitionOffsets, internalOffsets: PartitionOffsets): PartitionOffsets = {
    // kafka rebalancing can cause skewed views of partition ownership
    // by reconciling our offsets with another view, we can determine which we have ownership over
    currentOffsets = currentOffsets.filter { case (partition, offset) =>
      val externalOffset = externalOffsets.getOrElse(partition, -1L)
      offset >= externalOffset
    }
    internalOffsets filter { case (partition, offset) =>
      currentOffsets.contains(partition)
    } map { case (partition, offset) =>
      partition -> (offset + 1)
    }
  }

}
