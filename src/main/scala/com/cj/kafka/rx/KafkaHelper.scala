package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import org.apache.curator.utils.ZKPaths
import rx.lang.scala.Observable

object KafkaHelper {

  type TopicPartition = (String, Int)
  type OffsetMap = Map[TopicPartition, Long]
  type KafkaIterable = Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]]
  type KafkaObservable = Observable[Message[Array[Byte]]]

  type OffsetMerge = (OffsetMap, OffsetMap) => OffsetMap
  type PartialCommit = (OffsetMap, OffsetMerge) => OffsetMap
  type Commit = (OffsetMap, OffsetMerge, OffsetMerge) => OffsetMap

  // simple analog to kafka's ConsumerConfig
  case class SimpleConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false)

  def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)

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
  
  def copyMessage(message: MessageAndMetadata[Array[Byte], Array[Byte]], offsets: OffsetMap, checkpoint: PartialCommit) = {
    Message(value = message.message(), topic = message.topic, partition = message.partition, offset = message.offset, offsets = offsets, commitWith = checkpoint)
  }

}