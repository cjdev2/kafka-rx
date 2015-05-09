package com.cj.kafka

package object rx {

  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.Producer

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]

  type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]
  type RxMessage = Message[Array[Byte], Array[Byte]]

  type KafkaIterable = Iterable[KafkaMessage]
  type KafkaObservable = Observable[RxMessage]

  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type PartialCommit = (OffsetMap, OffsetMerge) => OffsetMap
  type Commit = (OffsetMap, OffsetMerge, OffsetMerge) => OffsetMap

  val defaultOffsets = Map[TopicPartition, Long]()
  val defaultPartialCommit: PartialCommit = { case (x, fn) => fn(x,x); x }
  
  def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

  private [rx] def copyMessage(message: KafkaMessage): RxMessage = {
    Message(
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset
    )
  }

  private [rx] def copyMessage(message: KafkaMessage, offsets: OffsetMap, checkpoint: PartialCommit) = {
    Message(
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset,
      offsets = offsets,
      partialCommit = checkpoint
    )
  }

  implicit class ProducerObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map { message =>
        val record = message.toProducerRecord(topic)
        val metadata = producer.send(record).get()
        val offsets = message.sourceMessage match {
          case Some(source) => source.offsets
          case None => Map[TopicPartition, Long]()
        }
        val partialCommit = message.sourceMessage match {
          case Some(source) => source.partialCommit
          case None => defaultPartialCommit
        }
        Message[K, V](
          key = record.key(),
          value = record.value(),
          partition = record.partition(),
          topic = record.topic(),
          offset = metadata.offset(),
          offsets = offsets,
          partialCommit = partialCommit
        )
      }
    }
  }

}
