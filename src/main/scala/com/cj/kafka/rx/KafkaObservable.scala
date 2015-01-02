package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import rx.lang.scala.Observable

object KafkaObservable {

  type KafkaStream = Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]]
  type KafkaObservable = Observable[Message[Array[Byte]]]

  def apply(stream: KafkaStream): KafkaObservable = {
    Observable
      .from(stream)
      .map(Message.fromKafka)
  }

  def apply(stream: KafkaStream, zk: OffsetCommitter): KafkaObservable = {
    val manager: OffsetManager[Array[Byte]] = new OffsetManager(commit = zk.commit)

    apply(stream)
      .map(manager.check)
      .filter(_.isDefined)
      .map(_.get)
  }

}