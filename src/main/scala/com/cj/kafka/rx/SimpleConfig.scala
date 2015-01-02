package com.cj.kafka.rx

case class SimpleConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false)
