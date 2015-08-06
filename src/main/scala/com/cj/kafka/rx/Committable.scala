package com.cj.kafka.rx

trait Committable[V] {
  def value: V
  def commit(offsetMerge: OffsetMerge): OffsetMap
  def commit(): OffsetMap = commit(defaultMerge)
}