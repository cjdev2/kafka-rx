package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import rx.lang.scala.Observable

object ObservableStream {

  type KafkaStream = Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]]
  type KafkaObservable = Observable[Message[Array[Byte]]]

  def apply(stream: KafkaStream): KafkaObservable = {
    Observable
      .from(stream)
      .map(Message.fromKafka)
  }

  def apply(stream: KafkaStream, zk: ZookeeperClient): KafkaObservable = {
    val manager: OffsetManager[Array[Byte]] = new OffsetManager(commit = zk.commit)
    zk.start()

    val observable = apply(stream)
      .map(manager.check)
      .filter(_.isDefined)
      .map(_.get)

    observable.doOnCompleted({ () => zk.close() })
    observable.doOnError({ err =>  zk.close() })

    observable

  }

}