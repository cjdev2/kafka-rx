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
  type Commit = (OffsetMerge) => OffsetMap

  private[rx] val defaultMerge: OffsetMerge = { case (theirs, ours) => ours }
  private[rx] val defaultOffsets = Map[TopicPartition, Long]()
  private[rx] val defaultCommit: Commit = { merge: OffsetMerge =>
    merge(defaultOffsets, defaultOffsets)
  }

  private[rx] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

  private[rx] def getMessage[K, V](
      message: MessageAndMetadata[K, V],
      commit: Commit = defaultCommit): Message[K, V] = {
    Message[K, V](
      key = message.key(),
      value = message.message(),
      topic = message.topic,
      partition = message.partition,
      offset = message.offset,
      commitfn = commit
    )
  }

  private[rx] def getResponseStream[K, V](
    producer: Producer[K, V],
    record: ProducerRecord[K, V],
    commit: Commit = defaultCommit): Observable[Message[K, V]] = {
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
              commitfn = commit
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
      stream.flatMap { message =>
        getResponseStream(
          producer,
          new ProducerRecord[K, V](topic, message.key, message.value),
          message.commitfn
        )
      }
    }
  }

  implicit class ProducerRecordObservable[K, V, k, v](stream: Observable[ProducerRecord[K, V]]) {
    def saveToKafka(producer: Producer[K, V]): Observable[Message[K, V]] = {
      stream.flatMap { record =>
        getResponseStream(producer, record)
      }
    }
  }

  implicit class ProducedMessageObservable[K, V](stream: Observable[ProducedMessage[K, V]]) {
    def saveToKafka(producer: Producer[K, V]): Observable[Message[K, V]] = {
      stream.flatMap { produced =>
        getResponseStream(
          producer,
          produced.record,
          produced.origin.commitfn
        )
      }
    }
  }

}
