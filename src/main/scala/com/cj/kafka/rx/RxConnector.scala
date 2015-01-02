package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer._
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryUntilElapsed
import rx.lang.scala.Observable

object RxConnector {
  def getConsumerConfig(config: SimpleConfig) = {
    val props = new Properties()
    props.put("group.id", config.group)
    props.put("zookeeper.connect", config.zookeepers)
    props.put("auto.offset.reset", if (config.startFromLatest) "largest" else "smallest")
    props.put("auto.commit.enable", if (config.autocommit) "true" else "false")
    new ConsumerConfig(props)
  }
}

class RxConnector(config: ConsumerConfig) {

  def this(config: SimpleConfig) = this(RxConnector.getConsumerConfig(config))
  def this(zookeepers: String, group: String) = this(SimpleConfig(zookeepers, group))
  
  private var kafkaClient: ConsumerConnector = null
  private var zkClient: CuratorFramework = null

  def getMessageStream(topic: String): Observable[Message[Array[Byte]]] = {
    connect()
    val kafkaStream: KafkaStream[Array[Byte], Array[Byte]] = kafkaClient.createMessageStreamsByFilter(new Whitelist(topic))(0)
    if (config.autoCommitEnable) {
      KafkaObservable(kafkaStream)
    } else {
      val zkCommitter = new OffsetCommitter(topic, config.groupId, zkClient)
      KafkaObservable(kafkaStream, zkCommitter)
    }
  }

  def connect() = {
    ensureKafkaConnection()
    if (!config.autoCommitEnable) ensureZookeeperConnection()
  }

  private def ensureKafkaConnection(): Unit = {
    if (kafkaClient == null) {
      kafkaClient = Consumer.create(config)
    }
  }

  private def ensureZookeeperConnection() = {
    if (zkClient == null) {
      zkClient = CuratorFrameworkFactory.newClient(config.zkConnect, new RetryUntilElapsed(10000, 250))
    }
    if (zkClient.getState != CuratorFrameworkState.STARTED) {
      zkClient.start()
      zkClient.blockUntilConnected()
    }
  }

  def shutdown() = {
    this.synchronized {
      if (kafkaClient != null) {
        kafkaClient.shutdown()
        kafkaClient = null
      }
      if (zkClient != null) {
        zkClient.close()
        zkClient = null
      }
    }
  }

}
