package com.cj.kafka

package object rx {

  import _root_.rx.lang.scala.subjects.PublishSubject
  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.{Producer, ProducerRecord, RecordMetadata, Callback}

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]

  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type MergeWith = (OffsetMap, OffsetMerge) => OffsetMap
  type Commit = (OffsetMap, OffsetMerge, OffsetMerge) => OffsetMap

  val defaultOffsets = Map[TopicPartition, Long]()
  val defaultMergeWith: MergeWith = {
    case (x, merge) => merge(x, x) // x should be the same as merge(x, x)
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

  private[rx] def copyMessage[K, V](message: MessageAndMetadata[K, V], offsets: OffsetMap, checkpoint: MergeWith): Message[K, V] = {
    Message[K, V](
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset,
      offsets = offsets,
      mergeWith = checkpoint
    )
  }

  implicit class MessageProducerObservable[K, V](stream: Observable[Message[K, V]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map(_.produce).saveToKafka(producer, topic)
    }
  }

  implicit class ProducerMessageObservable[K, V](stream: Observable[ProducerMessage[K, V]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.map(_.toProducerRecord(topic)).saveToKafka(producer)
    }
  }

  implicit class ProducerRecordObservable[K, V, k, v](stream: Observable[ProducerRecord[K, V]]) {
    def saveToKafka(producer: Producer[K, V]): Observable[Message[K, V]] = {
      stream.flatMap(getMetadata(producer, _))
    }
  }

  implicit class ProducedMessageObservable[K, V, k, v](stream: Observable[ProducedMessage[K, V, k, v]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.flatMap { message =>
        getMetadata(producer, message.toProducerRecord(topic)) map { msg =>
          msg.copy(
            offsets = message.sourceMessage.offsets,
            mergeWith = message.sourceMessage.mergeWith
          )
        }
      }
    }
  }

  def getMetadata[K, V](producer: Producer[K, V], record: ProducerRecord[K, V]): Observable[Message[K, V]] = {
    val subject = PublishSubject[Message[K, V]]()
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          subject.onError(exception)
        } else {
          subject.onNext(
            Message[K, V](
              key = record.key,
              value = record.value,
              partition = metadata.partition,
              topic = metadata.topic,
              offset = metadata.offset
            )
          )
        }
        subject.onCompleted()
      }
    })
    subject
  }

}
