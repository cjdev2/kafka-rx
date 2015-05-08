package com.cj.kafka

import org.apache.kafka.clients.producer.RecordMetadata

package object rx {

  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

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

  implicit class MessageTransformer[T](stream: Observable[Message[T]]) {
    def transform[K, V](fn: Message[T] => ProducerMessage[K, V]): Observable[ProducerMessage[K, V]] = stream map fn
  }

  implicit def toProducerRecord[K, V](message: ProducerMessage[K, V]): ProducerRecord[K, V] = {
    for {
      key <- message.key.orNull
      value <- message.value
      partition <- message.partition.orNull
      topic <- message.topic
      if message.valid
    } yield new ProducerRecord[K, V](topic, partition, key, value)
  }

  implicit class ProducerObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {
    def saveToKafka(topic: String)(implicit producer: Producer[K, V]): Observable[(RecordMetadata, OffsetMerge => OffsetMap)] = {
      stream.map { message =>
        val m: ProducerMessage[K, V] = message.copy(topic = Some(topic))
        m
      } map { message =>
        val metadata: RecordMetadata = producer.send(message).get()
        val function = message.commit
        (metadata, function)
      }

    }
    def transform[K2, V2](fn: ProducerMessage[K, V] => ProducerMessage[K2, V2]) = stream map fn
  }

}
