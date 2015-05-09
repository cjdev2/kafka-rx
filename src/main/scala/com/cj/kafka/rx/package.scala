package com.cj.kafka

import org.apache.kafka.clients.producer.RecordMetadata

package object rx {

  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.{ProducerRecord, Producer}

  import scala.language.implicitConversions

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type KafkaIterable = Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]]
  type KafkaObservable = Observable[Message[Array[Byte]]]

  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type PartialCommit = (OffsetMap, OffsetMerge) => OffsetMap
  type Commit = (OffsetMap, OffsetMerge, OffsetMerge) => OffsetMap


  def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

  def copyMessage(message: MessageAndMetadata[Array[Byte], Array[Byte]]): Message[Array[Byte]] = {
    Message(value = message.message(), topic = message.topic, partition = message.partition, offset = message.offset)
  }

  def copyMessage(message: MessageAndMetadata[Array[Byte], Array[Byte]], offsets: OffsetMap, checkpoint: PartialCommit) = {
    Message(value = message.message(), topic = message.topic, partition = message.partition, offset = message.offset, offsets = offsets, commitWith = checkpoint)
  }

  implicit class ProducerObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {

    private[rx] def toProducerRecord(topic: String, message: ProducerMessage[K, V]): ProducerRecord[K, V] = {
      val partition: Int = message.partition.asInstanceOf[Int]
      val key: K = message.key.asInstanceOf[K]
      val value: V = message.value
      new ProducerRecord(topic, partition, key, value)
    }

    def saveToKafka(producer: Producer[K, V], topic: String): Observable[ProducerResult[K, V]] = {
      stream.map { message =>
          val metadata: RecordMetadata = producer.send(toProducerRecord(topic, message)).get()
          val function = message.getCommitFn
          ProducerResult[K, V](metadata, message, function)
      }
    }
  }

}
