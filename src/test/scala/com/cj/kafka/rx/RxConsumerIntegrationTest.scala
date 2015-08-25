package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.producer.MockProducer
import org.scalatest._
import rx.lang.scala.subjects.{PublishSubject, ReplaySubject}
import rx.lang.scala.{Observer, Observable}
import rx.lang.scala.observables.ConnectableObservable

import collection.JavaConversions._

class RxConsumerIntegrationTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  "KafkaObservable" should "provide an observable stream" in {
    val conn = new RxConsumer("test", "test")
    val messages = getFakeKafkaMessages(2)

    val stream = conn.getObservableStream(messages)
    val list = stream.toBlocking.toList

    list.size should be(2)
    list.head.offset should be(0)
    list.last.offset should be(1)
  }

  it should "publish streams for multiple subscriptions" in {
    // kafka's iterable only supports single stateful iterator
    // by using `.publish` we can circumvent this and get a 'view' of the kafka iterator
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    val zk: ZookeeperOffsetCommitter = new ZookeeperOffsetCommitter("test", client)
    val conn = new RxConsumer("test", "test", committer = zk)
    try {
      server.start()
      val messages = getFakeKafkaMessages(10)
      val stream: ConnectableObservable[Long] = conn.getObservableStream(messages, zk).map(_.offset).publish

      val evenstream = stream.filter(x => (x % 2) == 0).take(5)
      val oddstream = stream.filter(x => (x % 2) == 1).take(5)

      // use subject to replay async behavior for tests
      val evenSubject = ReplaySubject[Long]()
      val oddSubject = ReplaySubject[Long]()
      evenstream.subscribe(evenSubject)
      oddstream.subscribe(oddSubject)

      stream.connect

      val evens = evenSubject.toBlocking.toList
      val odds = oddSubject.toBlocking.toList

      evens.size should be(5)
      odds.size should be(5)
      evens.map(_+1) should be(odds)
    } finally {
      server.close()
      client.close()
    }
  }

  it should "commit message offsets through zookeeper" in {
    val server = new TestingServer()
    val client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    val zk: ZookeeperOffsetCommitter = new ZookeeperOffsetCommitter("test", client)
    val conn = new RxConsumer("test", "test", committer = zk)
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

  it should "deliver messages to a producer" in {
    val fakeStream = Observable.from(getFakeKafkaMessages(10) map { msg => new Record(msg) })
    val producer = new MockProducer(true)
    val savedMessages = fakeStream.map(_.produce("test-topic")).saveToKafka(producer).toBlocking.toList
    val history = producer.history
    savedMessages.size should be(10)
    history.size should be(10)
    new String(history(5).value) should be("5")
  }

  def getFakeKafkaMessages(numMessages: Int): Iterable[MessageAndMetadata[Array[Byte], Array[Byte]]] = {
    val decoder = new DefaultDecoder()
    (0 to numMessages - 1) map { num =>
      val rawMessage = new kafka.message.Message(num.toString.getBytes)
      MessageAndMetadata(topic = num.toString, partition = num, offset = num.toLong, rawMessage = rawMessage, keyDecoder = decoder, valueDecoder = decoder)
    }
  }

}
