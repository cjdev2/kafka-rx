package com.cj.kafka.rx

case class ProducerMessage[K, V] (
  key: K,
  value: V,
  partition: Int,
  commitFn: Option[OffsetMerge => OffsetMap] = None
  ) {
  def getCommitFn: (OffsetMerge) => OffsetMap = commitFn match {
    case Some(fn) => fn
    case None => (_) => Map()
  }
}
