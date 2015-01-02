package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import org.apache.curator.utils.ZKPaths

object KafkaHelper {

  def extractPartition(path: String): Int = {
    val part = ZKPaths.getNodeFromPath(path)
    Integer.parseInt(part)
  }

  def getPartitionPath(base: String, part: Int) = ZKPaths.makePath(base, part.toString)

  def getConsumerOffsetPath(topic: String, group: String) = s"/consumers/$group/offsets/$topic"

  def getConsumerConfig(config: SimpleConfig) = {
    val props = new Properties()
    props.put("group.id", config.group)
    props.put("zookeeper.connect", config.zookeepers)
    props.put("auto.offset.reset", if (config.startFromLatest) "largest" else "smallest")
    props.put("auto.commit.enable", if (config.autocommit) "true" else "false")
    new ConsumerConfig(props)
  }

  def copyMessage(message: MessageAndMetadata[Array[Byte], Array[Byte]]): Message[Array[Byte]] = {
    Message(value=message.message(), topic=message.topic, partition=message.partition, offset=message.offset)
  }

}