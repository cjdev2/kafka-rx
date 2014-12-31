package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.curator.test.TestingServer
import org.scalatest._
import org.scalatest.mock.MockitoSugar
import rx.lang.scala.Observable

class ObservableStreamIntegrationTest extends FlatSpec with ShouldMatchers with BeforeAndAfter with MockitoSugar {

  "ZKObservableStream" should "provide an observable stream" in {
    val client: ZookeeperClient = mock[ZookeeperClient]
    val messages = getFakeKafkaMessages(2)

    val stream = ObservableStream(messages, client)
    val list = stream.toBlocking.toList

    list.size should be(2)
    list.head.offsets should be(Map(1 -> 1L))
    list.last.offsets should be(Map(1 -> 1L, 2 -> 2L))
  }

  it should "only support a single iterator, like a kafka stream" in {
    val client: ZookeeperClient = mock[ZookeeperClient]
    val messages = getFakeKafkaMessages(10)

    val stream: Observable[Long] = ObservableStream(messages, client).map(_.offset)

    val evens = stream.filter(x => (x % 2) == 0).toBlocking.toList
    val odds = stream.filter(x => (x % 2) == 1).toBlocking.toList

    evens.size should be(5)
    odds.size should be(0)
  }

  it should "commit offsets to zookeeper through message checkpoints" in {
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    try {
      server.start()
      client.start()
      val zk: ZookeeperClient = new ZookeeperClient("test", "test", client)
      val fakeMessages = getFakeKafkaMessages(10)
      val stream: Observable[Message[Long]] = ObservableStream(fakeMessages, zk) map { message =>
        message.copy(value = message.offset)
      }
      val messages = stream.toBlocking.toList
      messages.last.checkpoint()
      zk.getOffsets should be(messages.last.offsets)
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
