package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer.ConsumerConfig

case class SimpleConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false) {
  def getConsumerConfig = {
    val props = new Properties()
    props.put("group.id", group)
    props.put("producer.type", "async")
    props.put("zookeeper.connect", zookeepers)
    props.put("auto.offset.reset", if (startFromLatest) "largest" else "smallest")
    props.put("auto.commit.enable", if (autocommit) "true" else "false")
    new ConsumerConfig(props)
  }

}
