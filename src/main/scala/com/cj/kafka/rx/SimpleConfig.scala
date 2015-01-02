package com.cj.kafka.rx

// simple analog to kafka's ConsumerConfig
case class SimpleConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false)
