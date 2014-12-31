package com.cj.kafka.rx

import org.scalatest._

class ZKHelperTest extends FlatSpec with ShouldMatchers {

  "ZKHelper" should "calculate a zookeeper path for a consumer" in {
    val topic = "topic"
    val group = "consumer-group"

    val path = ZKHelper.getConsumerOffsetPath(topic, group)

    path should be(s"/consumers/$group/offsets/$topic")
  }

  it should "extract partitions from zookeeper paths" in {
    val expectedPartition = 1337

    val path = ZKHelper.getPartitionPath("/test/path", expectedPartition)
    val partition = ZKHelper.extractPartition(path)

    partition should be(expectedPartition)
  }

}
