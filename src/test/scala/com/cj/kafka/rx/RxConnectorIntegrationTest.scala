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
    val conn = new RxConnector("test", "test")
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    try {
      server.start()
      client.start()
      val zk: OffsetCommitter = new OffsetCommitter("test", "test", client)
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

  it should "commit offsets to zookeeper through message checkpoints" in {
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    val conn = new RxConnector("test", "test")
    try {
      server.start()
      client.start()
      val zk: OffsetCommitter = new OffsetCommitter("test", "test", client)
      val fakeMessages = getFakeKafkaMessages(10)
      val stream: Observable[Message[Long]] = conn.getObservableStream(fakeMessages, zk) map { message =>
        message.copy(value = message.offset)
      }
      val messages = stream.toBlocking.toList
      messages.last.checkpoint()
      val adjustedOffsets = messages.last.offsets map { case (partition, offset) =>
        partition -> (offset + 1)
      }
      zk.getOffsets should be(adjustedOffsets)
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
