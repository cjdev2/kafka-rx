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

  implicit class ProducerObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {
    private[rx] def toProducerRecord[K, V](message: ProducerMessage[K, V]): ProducerRecord[K, V] = {
      val topic: String = message.topic.get
      val partition: Int = message.partition.getOrElse(null).asInstanceOf[Int]
      val key: K = message.key.getOrElse(null).asInstanceOf[K]
      val value: V = message.value.get
      new ProducerRecord[K, V](topic, partition, key, value)
    }

    def saveToKafka(topic: String)(implicit producer: Producer[K, V]): Observable[ProducerResult] = {
      stream.map { message =>
        message.copy(topic = Some(topic)).asInstanceOf[ProducerMessage[K, V]]
      } map { message =>
          val metadata: RecordMetadata = producer.send(toProducerRecord(message)).get()
          val function = message.commit
          ProducerResult(metadata, function)
      }
    }
    def transform[K2, V2](fn: ProducerMessage[K, V] => ProducerMessage[K2, V2]) = stream map fn
  }

}
