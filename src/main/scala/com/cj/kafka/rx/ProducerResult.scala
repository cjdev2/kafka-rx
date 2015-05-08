package com.cj.kafka.rx

import org.apache.kafka.clients.producer.RecordMetadata


case class ProducerResult(topic: String, partition: Int, offset: Long, commit: OffsetMerge => OffsetMap)


object ProducerResult {
    def apply(recordMetadata: RecordMetadata, commitFn: OffsetMerge => OffsetMap) = new ProducerResult(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), commitFn)
}