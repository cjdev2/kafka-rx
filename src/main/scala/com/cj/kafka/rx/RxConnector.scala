package com.cj.kafka.rx

import kafka.consumer._
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryUntilElapsed
import rx.lang.scala.Observable

class RxConnector(config: ConsumerConfig) {

  def this(config: SimpleConfig) = this(getConsumerConfig(config))
  def this(zookeepers: String, group: String) = this(SimpleConfig(zookeepers, group))
  
  private var kafkaClient: ConsumerConnector = null
  private var zkClient: CuratorFramework = null

  def getMessageStream(topic: String) = getMessageStreams(topic)(0)
  def getMessageStreams(topic: String, numStreams: Int = 1): Seq[KafkaObservable] = {
    connect()
    val kafkaStreams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = kafkaClient.createMessageStreamsByFilter(new Whitelist(topic), numStreams = numStreams)
    if (config.autoCommitEnable) {
      kafkaStreams.map(getObservableStream)
    } else {
      val zkCommitter = new OffsetCommitter(config.groupId, zkClient)
      kafkaStreams.map({ case stream =>
        getObservableStream(stream, zkCommitter)
      })
    }
  }

  protected[rx] def getObservableStream(stream: KafkaIterable): KafkaObservable = {
    Observable
      .from(stream)
      .map(copyMessage)
  }

  protected[rx] def getObservableStream(stream: KafkaIterable, zk: OffsetCommitter): KafkaObservable = {
    val manager = new OffsetManager(commit = zk.commit)
    Observable
      .from(stream)
      .map(manager.check)
      .filter(_.isDefined)
      .map(_.get)
  }

  def connect() = {
    this.synchronized {
      ensureKafkaConnection()
      if (!config.autoCommitEnable) ensureZookeeperConnection()
    }
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
