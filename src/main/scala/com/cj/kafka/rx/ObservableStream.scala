package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import rx.lang.scala.Observable

object ObservableStream {

  def apply(stream: Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]]): Observable[Message[Array[Byte]]] = {
    Observable
      .from(stream)
      .map(Message.fromKafka)
  }

  def apply(stream: Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]], zk: ZookeeperClient): Observable[Message[Array[Byte]]] = {
    lazy val manager: OffsetManager[Array[Byte]] = new OffsetManager[Array[Byte]](checkpointFn = committer.commit)
    lazy val committer = new ZookeeperCommitter(zk, manager)

    val observable = apply(stream)
      .map(manager.check)
      .filter(_.isDefined)
      .map(_.get)

    observable.doOnCompleted({ () => zk.close() })
    observable.doOnError({ err => zk.close() })

    zk.start()

    observable

  }

  class ZookeeperCommitter(zk: ZookeeperClient, manager: OffsetManager[Array[Byte]]) {
    def commit(offsets: Map[Int, Long]): Map[Int, Long] = {
      val lock = zk.getLock
      lock.acquire()
      try {
        val zkOffsets = zk.getOffsets
        val adjustedOffsets = manager.adjustOffsets(zkOffsets, offsets)
        zk.setOffsets(adjustedOffsets)
      } finally {
        lock.release()
      }
    }
  }

}