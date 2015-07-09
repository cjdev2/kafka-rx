package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import org.scalatest.{FlatSpec, ShouldMatchers}

class OffsetManagerTest extends FlatSpec with ShouldMatchers {

  type RxMessage = Message[Array[Byte], Array[Byte]]
  type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]

  object FakeOffsetCommitter extends OffsetCommitter {
    var committedOffsets: OffsetMap = Map()
    def getOffsets(topicPartitions: Iterable[(String, Int)]): OffsetMap = committedOffsets
    def setOffsets(offsets: OffsetMap): OffsetMap = {
      committedOffsets = offsets
      committedOffsets
    }
    def commit(offsets: OffsetMap, merge: OffsetMerge): OffsetMap = {
      merge(getOffsets(offsets.keys), offsets)
    }
  }

  "OffsetManager" should "begins with no offsets" in {
    val offsets = new OffsetManager(FakeOffsetCommitter)
    offsets.getOffsets should be(Map[Int, Long]())
  }

  it should "record offsets for kafka messages" in {
    // given
    val offsets = new OffsetManager[Array[Byte], Array[Byte]](FakeOffsetCommitter)
    val message = Message(key="test message".getBytes, value="test message".getBytes, topic="test-topic", partition=0, offset=1L)

    // when
    val checkedMessage = offsets.check(kafkaRawMessage(message))

    // then
    offsets.getOffsets should be(Map(message.topicPartition -> message.offset))
    checkedMessage.isDefined should be(true)
  }

  it should "not record the same message twice" in {
    // given
    val manager = new OffsetManager[Array[Byte], Array[Byte]](FakeOffsetCommitter)
    val message: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)

    // when
    val firstAttempt = manager.check(kafkaRawMessage(message))
    val secondAttempt = manager.check(kafkaRawMessage(message))

    // then
    firstAttempt should be(Some(message))
    secondAttempt should be(None)
  }

  it should "not update offsets for old messages" in {
    // given
    val manager = new OffsetManager[Array[Byte], Array[Byte]](FakeOffsetCommitter)
    val oldMessage: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val newMessage: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=1L)

    // when
    val newAttempt = manager.check(kafkaRawMessage(newMessage))
    val oldAttempt = manager.check(kafkaRawMessage(oldMessage))

    // then
    manager.getOffsets should be(Map(newMessage.topicPartition -> newMessage.offset))
    newAttempt should be(Some(newMessage))
    oldAttempt should be(None)
  }

  it should "keep track across multiple partitions" in {
    // given
    val offsets = new OffsetManager[Array[Byte], Array[Byte]](FakeOffsetCommitter)
    val messageA: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val messageB: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=1, offset=0L)

    // when
    offsets.check(kafkaRawMessage(messageA))
    offsets.check(kafkaRawMessage(messageB))

    // then
    offsets.getOffsets should be(Map(messageA.topicPartition -> messageA.offset, messageB.topicPartition -> messageB.offset))
  }

  it should "know which partitions it owns" in {
    // given
    val offsets = new OffsetManager[Array[Byte], Array[Byte]](FakeOffsetCommitter)
    val messageA: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val messageB: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=1, offset=0L)
    offsets.check(kafkaRawMessage(messageA))
    offsets.check(kafkaRawMessage(messageB))

    // when
    val partitions = offsets.getOffsets.keySet

    // then
    partitions should be(Set(messageA.topicPartition, messageB.topicPartition))
  }
  it should "provide its checkpoint function to all the messages" in {

    val failingMerge: Commit = { case _ =>
      throw new RuntimeException("This function should never be called")
      Map[TopicPartition, Long]()
    }

    object HappyCommitter extends OffsetCommitter {
      def getOffsets(topicPartitions: Iterable[(String, Int)]): OffsetMap = ???
      def setOffsets(offsets: OffsetMap): OffsetMap = ???
      def getPartitionLock(topicPartition: (String, Int)): PartitionLock = ???
      override def commit(offsets: OffsetMap, userMerge: OffsetMerge): OffsetMap = {
        "this function should pass".split(" ") should contain("pass")
        offsets
      }
    }

    val message: RxMessage = Message(value="test".getBytes, topic="test-topic", partition=0, offset=0L, commitfn=failingMerge)
    val manager = new OffsetManager[Array[Byte], Array[Byte]](offsetCommitter=HappyCommitter)

    val checkedMessage = manager.check(kafkaRawMessage(message))
    checkedMessage.get.commit() // should not call the failingFn, since the offset manager should have provided the passingFn
  }

  def kafkaRawMessage(message: RxMessage): KafkaMessage = {
    val msg = new kafka.message.Message(message.value)
    val decoder = new kafka.serializer.DefaultDecoder
    MessageAndMetadata(message.topic, message.partition, msg, message.offset, decoder, decoder)
  }

}
