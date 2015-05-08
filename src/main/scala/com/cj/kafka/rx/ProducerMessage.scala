package com.cj.kafka.rx

case class ProducerMessage[K, V] (
  key: Option[K] = None,
  value: Option[V] = None,
  partition: Option[Int] = None,
  topic: Option[String] = None,
  commitFn: Option[OffsetMerge => OffsetMap] = None
  ) {
  def valid: Boolean = value.isDefined && topic.isDefined
  def commit: (OffsetMerge) => OffsetMap = commitFn match {
    case Some(fn) => fn
    case None => (_:OffsetMerge) => Map()
  }
}
