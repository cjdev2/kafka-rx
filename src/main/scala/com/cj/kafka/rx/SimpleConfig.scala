package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer.ConsumerConfig

case class SimpleConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false) {
  def getConsumerConfig(config: SimpleConfig) = {
    val props = new Properties()
    props.put("group.id", config.group)
    props.put("zookeeper.connect", config.zookeepers)
    props.put("auto.offset.reset", if (config.startFromLatest) "largest" else "smallest")
    props.put("auto.commit.enable", if (config.autocommit) "true" else "false")
    new ConsumerConfig(props)
  }

}
