package com.cj.kafka.rx

import org.apache.kafka.clients.producer.RecordMetadata

case class ProducerResult[K, V](recordMetadata: RecordMetadata, message: ProducerMessage[K, V], private val commitFn: OffsetMerge => OffsetMap) {
    def commit = { commitFn({ (zkOffsets, internalOffsets) =>  internalOffsets }) }
    def topic: String = recordMetadata.topic()
    def offset: Long = recordMetadata.offset()
    def partition: Int = recordMetadata.partition()
    def key: K = message.key
    def value: V = message.value
}