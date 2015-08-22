package com.cj.kafka.rx

trait Committable[V] {
  private[rx] def commitfn: Commit = defaultCommit
  def value: V
  def commit(offsetMerge: OffsetMerge): OffsetMap
  def commit(): OffsetMap = commit(defaultMerge)
  def derive[R](newValue: R): Committable[R] = {
    val origin = this
    new Committable[R] {
      def value: R = newValue
      def commit(offsetMerge: OffsetMerge): OffsetMap = origin.commit(offsetMerge)
      override def commit(): OffsetMap = origin.commit()
    }
  }
  def map[R](fn: (V) => R) : Committable[R] = {
    derive(fn(value))
  }
}