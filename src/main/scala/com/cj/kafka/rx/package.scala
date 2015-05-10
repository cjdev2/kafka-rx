package com.cj.kafka


package object rx {

  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]

  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type PartialCommit = (OffsetMap, OffsetMerge) => OffsetMap
  type Commit = (OffsetMap, OffsetMerge, OffsetMerge) => OffsetMap

  val defaultOffsets = Map[TopicPartition, Long]()
  val defaultPartialCommit: PartialCommit = {
    case (x, fn) => fn(x, x); x
  }

  def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

  private[rx] def copyMessage[K, V](message: MessageAndMetadata[K, V]): Message[K, V] = {
    Message[K, V](
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset
    )
  }

  private[rx] def copyMessage[K, V](message: MessageAndMetadata[K, V], offsets: OffsetMap, checkpoint: PartialCommit): Message[K, V] = {
    Message[K, V](
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset,
      offsets = offsets,
      partialCommit = checkpoint
    )
  }

  implicit class MessageProducerObservable[K, V](stream: Observable[Message[K, V]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map { message =>
        message.produce[K, V](key = message.key, value = message.value, partition = message.partition)
      }.saveToKafka(producer, topic)
    }
  }

  implicit class ProducerMessageObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map { message =>
        val record = message.toProducerRecord(topic)
        val metadata = producer.send(record).get()
        Message[K, V](
          key = record.key,
          value = record.value,
          partition = metadata.partition,
          topic = metadata.topic,
          offset = metadata.offset
        )
      }
    }
  }

  implicit class ProducerRecordObservable[K, V, k, v](stream: Observable[ProducerRecord[K, V]]) {
    def saveToKafka(producer: Producer[K, V]): Observable[Message[K, V]] = {
      stream.map { record: ProducerRecord[K, V] =>
        val metadata = producer.send(record).get()
        Message[K, V](
          key = record.key,
          value = record.value,
          partition = metadata.partition,
          topic = metadata.topic,
          offset = metadata.offset
        )
      }
    }
  }

  implicit class ProducedMessageObservable[K, V, k, v](stream: Observable[ProducedMessage[K, V, k, v]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map { message =>
        val record = message.toProducerRecord(topic)
        val metadata = producer.send(record).get()
        Message[K, V](
          key = record.key,
          value = record.value,
          partition = metadata.partition,
          topic = metadata.topic,
          offset = metadata.offset,
          offsets = message.sourceMessage.offsets,
          partialCommit = message.sourceMessage.partialCommit
        )
      }
    }
  }

}
