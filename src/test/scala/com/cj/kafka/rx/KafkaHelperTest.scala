package com.cj.kafka.rx

import org.scalatest._

class KafkaHelperTest extends FlatSpec with ShouldMatchers {

  "ZKHelper" should "calculate a zookeeper path for a consumer" in {
    val topic = "topic"
    val group = "consumer-group"

    val path = KafkaHelper.getConsumerOffsetPath(topic, group)

    path should be(s"/consumers/$group/offsets/$topic")
  }

  it should "extract partitions from zookeeper paths" in {
    val expectedPartition = 1337

    val path = KafkaHelper.getPartitionPath("/test/path", expectedPartition)
    val partition = KafkaHelper.extractPartition(path)

    partition should be(expectedPartition)
  }

}
