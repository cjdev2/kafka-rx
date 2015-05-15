package com.cj.kafka.rx

import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, Decoder}
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryUntilElapsed
import rx.lang.scala.Observable

class RxConnector(config: ConsumerConfig) {

  def this(config: SimpleConfig) = this(config.getConsumerConfig)
  def this(zookeepers: String, group: String) = this(SimpleConfig(zookeepers, group))
  
  private var kafkaClient: ConsumerConnector = null
  private var zkClient: CuratorFramework = null

  def getMessageStream[K, V](topic: String, keyDecoder: Decoder[K] = new DefaultDecoder, valueDecoder: Decoder[V] = new DefaultDecoder) = {
    getMessageStreams(topic, 1, keyDecoder, valueDecoder)(0)
  }

  def getMessageStreams[K, V](topic: String, numStreams: Int = 1, keyDecoder: Decoder[K] = new DefaultDecoder, valueDecoder: Decoder[V] = new DefaultDecoder) = {
    connect()
    val kafkaStreams: Seq[KafkaStream[K, V]] = kafkaClient.createMessageStreamsByFilter[K, V](
      new Whitelist(topic),
      numStreams = numStreams,
      keyDecoder = keyDecoder,
      valueDecoder = valueDecoder
    )
    if (config.autoCommitEnable) {
      kafkaStreams.map(getObservableStream[K, V])
    } else {
      val zkCommitter = new OffsetCommitter(config.groupId, zkClient)
      kafkaStreams.map({ case stream =>
        getObservableStream[K, V](stream, zkCommitter)
      })
    }
  }

  protected[rx] def getObservableStream[K, V](stream: Iterable[MessageAndMetadata[K, V]]): Observable[Message[K, V]] = {
    Observable
      .from(stream)
      .map(copyMessage[K, V](_))
  }

  protected[rx] def getObservableStream[K, V](stream: Iterable[MessageAndMetadata[K, V]], zk: OffsetCommitter): Observable[Message[K, V]] = {
    val manager = new OffsetManager[K, V](commit = zk.commit)
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
