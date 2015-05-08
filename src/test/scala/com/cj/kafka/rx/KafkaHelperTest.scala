package com.cj.kafka.rx

import org.scalatest._

class KafkaHelperTest extends FlatSpec with ShouldMatchers {

  "ZKHelper" should "calculate a zookeeper path for a consumer" in {
    val topic = "topic"
    val group = "consumer-group"

    val path = getPartitionPath(group, topic, 1)

    path should be(s"/consumers/$group/offsets/$topic/1")
  }

}
