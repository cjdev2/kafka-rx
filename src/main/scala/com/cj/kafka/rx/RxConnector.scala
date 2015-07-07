package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, Decoder}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import rx.lang.scala.Observable

class RxConnector(config: ConsumerConfig, committer: OffsetCommitter) {

  def this(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false, committer: OffsetCommitter = null) =
    this(RxConnector.getConsumerConfig(zookeepers, group, autocommit, startFromLatest), committer)

  private var kafkaConsumer: ConsumerConnector = null
  private var offsetCommitter: OffsetCommitter = committer

  def getMessageStream[K, V](topic: String, keyDecoder: Decoder[K] = new DefaultDecoder, valueDecoder: Decoder[V] = new DefaultDecoder) = {
    getMessageStreams(topic, 1, keyDecoder, valueDecoder)(0)
  }

  def getMessageStreams[K, V](topic: String, numStreams: Int = 1, keyDecoder: Decoder[K] = new DefaultDecoder, valueDecoder: Decoder[V] = new DefaultDecoder) = {
    val kafkaStreams: Seq[KafkaStream[K, V]] = ensureKafkaConsumer().createMessageStreamsByFilter[K, V](
      new Whitelist(topic),
      numStreams = numStreams,
      keyDecoder = keyDecoder,
      valueDecoder = valueDecoder
    )
    if (config.autoCommitEnable) {
      kafkaStreams.map(getObservableStream[K, V])
    } else {
      kafkaStreams.map({ case stream =>
        getObservableStream[K, V](stream, ensureOffsetCommitter())
      })
    }
  }

  protected[rx] def getObservableStream[K, V](stream: Iterable[MessageAndMetadata[K, V]]): Observable[Message[K, V]] = {
    Observable
      .from(stream)
      .map(getMessage[K, V](_))
  }

  protected[rx] def getObservableStream[K, V](stream: Iterable[MessageAndMetadata[K, V]], zk: OffsetCommitter): Observable[Message[K, V]] = {
    offsetCommitter.start()
    val manager = new OffsetManager[K, V](zk)
    Observable
      .from(stream)
      .map(manager.check)
      .filter(_.isDefined)
      .map(_.get)
  }

  private def ensureKafkaConsumer(): ConsumerConnector = {
    if (kafkaConsumer == null) {
      kafkaConsumer = Consumer.create(config)
    }
    kafkaConsumer
  }

  private def ensureOffsetCommitter(): OffsetCommitter = {
    if (offsetCommitter == null) {
      offsetCommitter = RxConnector.getZKCommitter(config)
    }
    offsetCommitter
  }

  def shutdown() = {
    this.synchronized {
      if (kafkaConsumer != null) {
        kafkaConsumer.shutdown()
        kafkaConsumer = null
      }
      if (offsetCommitter != null) {
        offsetCommitter.stop()
        offsetCommitter = null
      }
    }
  }
}

object RxConnector {
  private[rx] def getZKCommitter(config: ConsumerConfig) = {
    new ZookeeperOffsetCommitter(
      config.groupId,
      CuratorFrameworkFactory.newClient(config.zkConnect, new ExponentialBackoffRetry(256, 1024))
    )
  }
  private[rx] def getConsumerConfig(zookeepers: String, group: String, autocommit: Boolean = false, startFromLatest: Boolean = false) = {
    val props = new Properties()
    props.put("group.id", group)
    props.put("zookeeper.connect", zookeepers)
    props.put("auto.offset.reset", if (startFromLatest) "largest" else "smallest")
    props.put("auto.commit.enable", if (autocommit) "true" else "false")
    new ConsumerConfig(props)
  }
}