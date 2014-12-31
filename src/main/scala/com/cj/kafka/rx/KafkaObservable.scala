package com.cj.kafka.rx

import java.util.Properties

import kafka.consumer._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed

import rx.lang.scala.Observable

object KafkaObservable {

  def getByteStream(zookeepers: String, topic: String, client: String, autoCommit: Boolean = false): Observable[Message[Array[Byte]]] = {
    val stream = getKafkaStream(zookeepers, topic, client, autoCommit)
    if (autoCommit) {
      ObservableStream(stream)
    } else {
      ObservableStream(stream, getZookeeperClient(zookeepers, topic, client))
    }
  }

  private def getKafkaStream(zookeepers: String, topic: String, client: String, autoCommit: Boolean): KafkaStream[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("group.id", client)
    props.put("zookeeper.connect", zookeepers)
    if (! autoCommit) {
      props.put("auto.commit.enable", "false")
    }
    val consumerOpts = new ConsumerConfig(props)
    val connector: ConsumerConnector = Consumer.create(consumerOpts)
    val filter = new Whitelist(topic)
    connector.createMessageStreamsByFilter(filter)(0)
  }

  private def getZookeeperClient(zookeepers: String, topic: String, client: String) = {
    val zk = CuratorFrameworkFactory.newClient(zookeepers,  new RetryUntilElapsed(10000, 250))
    new ZookeeperClient(topic, client, zk)
  }

}
