package com.cj.kafka.rx


import kafka.consumer._
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryUntilElapsed
import rx.lang.scala.Observable

class RxConnector(config: ConsumerConfig) {

  def this(config: SimpleConfig) = this(KafkaHelper.getConsumerConfig(config))
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
