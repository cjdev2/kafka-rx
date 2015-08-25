package com.cj.kafka.rx

import kafka.message.MessageAndMetadata

class OffsetManager[K, V](offsetCommitter: OffsetCommitter) {

  // Offset Manager keeps track of offsets per partition for a particular kafka stream
  private var currentOffsets = Map[TopicPartition, Long]()
  def getOffsets: OffsetMap = currentOffsets

  def check(message: MessageAndMetadata[K, V]): Option[Record[K, V]] = {
    // updates internal offset state & determines whether the message is stale due to replay
    currentOffsets.get(message.topic -> message.partition) match {
      case None => manageMessage(message)
      case Some(priorOffset) =>
        if (message.offset > priorOffset) manageMessage(message)
        else None
    }
  }

  def manage(externalOffsets: OffsetMap, internalOffsets: OffsetMap): OffsetMap = {
    // kafka rebalancing can cause skewed views of partition ownership
    currentOffsets = currentOffsets.filter { case (topicPartition, internalOffset) =>
      // we only own those topic partitions where our offsets are higher then external
      val externalOffset = externalOffsets.getOrElse(topicPartition, -1L)
      internalOffset >= externalOffset
    }
    internalOffsets filter { case (topicPartition, offset) =>
      currentOffsets.contains(topicPartition)
    }
  }

  def commitWith(internalOffsets: OffsetMap)(merge: OffsetMerge): OffsetMap = {
    offsetCommitter.commit(internalOffsets, { case (externalOffsets, _) =>
      manage(externalOffsets, merge(externalOffsets, internalOffsets))
    })
  }

  private def manageMessage(msg:  MessageAndMetadata[K, V]): Some[Record[K, V]] = {
    currentOffsets += msg.topic -> msg.partition -> msg.offset
    Some(new Record(msg, commitWith(currentOffsets)))
  }

}
