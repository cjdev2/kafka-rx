package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.curator.test.TestingServer
import org.scalatest._
import rx.lang.scala.Observable

class RxConnectorIntegrationTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  "KafkaObservable" should "provide an observable stream" in {
    val conn = new RxConnector("test", "test")
    val messages = getFakeKafkaMessages(2)

    val stream = conn.getObservableStream(messages)
    val list = stream.toBlocking.toList

    list.size should be(2)
    list.head.offset should be(1)
    list.last.offset should be(2)
  }

  it should "cache iterables for subsequent iteration" in {
    // kafka's iterable only supports single stateful iterator
    // by having a cache we can circumvent this and get a 'view' of the kafka iterator
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    val zk: ZookeeperOffsetCommitter = new ZookeeperOffsetCommitter("test", client)
    val conn = new RxConnector("test", "test", committer = zk)
    try {
      server.start()
      val messages = getFakeKafkaMessages(10)
      val stream: Observable[Long] = conn.getObservableStream(messages, zk).map(_.offset).cache(10)

      val evens = stream.filter(x => (x % 2) == 0).toBlocking.toList
      val odds = stream.filter(x => (x % 2) == 1).toBlocking.toList

      evens.size should be(5)
      odds.size should be(5)
      odds.map(_+1) should be(evens)
    } finally {
      server.close()
      client.close()
    }
  }

  it should "commit message offsets through zookeeper" in {
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    val zk: ZookeeperOffsetCommitter = new ZookeeperOffsetCommitter("test", client)
    val conn = new RxConnector("test", "test", committer = zk)
    val fakeMessages = getFakeKafkaMessages(10)
    val topicPartitions = fakeMessages map { message => message.topic -> message.partition}
    val expectedOffsets = (topicPartitions map { case (topic, partition) => topic -> partition -> (partition + 1) }).toMap
    try {
      server.start()
      val stream = conn.getObservableStream(fakeMessages, zk) map { message =>
        message.copy(value = message.offset)
      }
      stream.toBlocking.toList.last.commit() should be(expectedOffsets)
      zk.getOffsets(topicPartitions) should be(expectedOffsets)
    } finally {
      server.close()
      client.close()
    }
  }

  def getFakeKafkaMessages(numMessages: Int): Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]] = {
    val decoder = new DefaultDecoder()
    (1 to numMessages) map { num =>
      val rawMessage = new kafka.message.Message(num.toString.getBytes)
      MessageAndMetadata(topic = num.toString, partition = num, offset = num.toLong, rawMessage = rawMessage, keyDecoder = decoder, valueDecoder = decoder)
    }
  }

}
