package com.cj.kafka

package object rx {

  import _root_.rx.lang.scala.subjects.AsyncSubject
  import _root_.rx.lang.scala.Observable
  import kafka.message.MessageAndMetadata
  import org.apache.curator.utils.ZKPaths
  import org.apache.kafka.clients.producer.{Producer, ProducerRecord, RecordMetadata, Callback}

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type PartialCommit = (OffsetMerge) => OffsetMap

  private[rx] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[rx] val defaultOffsets = Map[TopicPartition, Long]()
  private[rx] val defaultCommit: PartialCommit = { merge: OffsetMerge =>
    merge(defaultOffsets, defaultOffsets)
  }

  private[rx] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

  private[rx] def getMessage[K, V](
      message: MessageAndMetadata[K, V],
      commit: PartialCommit = defaultCommit): Message[K, V] = {
    Message[K, V](
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset,
      commitWith = commit
    )
  }

  private[rx] def getResponseStream[K, V](
    producer: Producer[K, V],
    record: ProducerRecord[K, V],
    commit: PartialCommit = defaultCommit): Observable[Message[K, V]] = {
    val subject = AsyncSubject[Message[K, V]]()
    producer.send(record, new Callback {
      def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          subject.onError(exception)
        } else {
          subject.onNext(
            Message[K, V](
              key = record.key,
              value = record.value,
              partition = metadata.partition,
              topic = metadata.topic,
              offset = metadata.offset,
              commitWith = commit
            )
          )
        }
        subject.onCompleted()
      }
    })
    subject
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
      stream.flatMap(getResponseStream(producer, _))
    }
  }

  implicit class ProducedMessageObservable[K, V, k, v](stream: Observable[ProducedMessage[K, V, k, v]]) {
    def saveToKafka(producer: Producer[K, V], topic: String): Observable[Message[K, V]] = {
      stream.flatMap { message =>
        getResponseStream(
          producer,
          message.toProducerRecord(topic),
          message.sourceMessage.commitWith
        )
      }
    }
  }

}
