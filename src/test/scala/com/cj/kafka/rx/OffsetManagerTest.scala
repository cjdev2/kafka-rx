package com.cj.kafka.rx

import kafka.message.MessageAndMetadata
import org.scalatest.{FlatSpec, ShouldMatchers}

class OffsetManagerTest extends FlatSpec with ShouldMatchers {

  type RxMessage = Message[Array[Byte], Array[Byte]]
  type KafkaMessage = MessageAndMetadata[Array[Byte], Array[Byte]]

  "OffsetManager" should "begins with no offsets" in {
    val offsets = new OffsetManager
    offsets.getOffsets should be(Map[Int, Long]())
  }

  it should "record offsets for kafka messages" in {
    // given
    val offsets = new OffsetManager[Array[Byte], Array[Byte]]
    val message = Message(key="test message".getBytes, value="test message".getBytes, topic="test-topic", partition=0, offset=1L)

    // when
    val checkedMessage = offsets.check(kafkaRawMessage(message))

    // then
    offsets.getOffsets should be(Map(message.topicPartition -> message.offset))
    checkedMessage.get.offsets should be(offsets.getOffsets)
  }

  it should "not record the same message twice" in {
    // given
    val manager = new OffsetManager[Array[Byte], Array[Byte]]
    val message: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)

    // when
    val firstAttempt = manager.check(kafkaRawMessage(message))
    val secondAttempt = manager.check(kafkaRawMessage(message))

    // then
    firstAttempt should be(Some(message.copy(offsets = manager.getOffsets)))
    secondAttempt should be(None)
  }

  it should "not update offsets for old messages" in {
    // given
    val manager = new OffsetManager[Array[Byte], Array[Byte]]
    val oldMessage: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val newMessage: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=1L)

    // when
    val newAttempt = manager.check(kafkaRawMessage(newMessage))
    val oldAttempt = manager.check(kafkaRawMessage(oldMessage))

    // then
    manager.getOffsets should be(Map(newMessage.topicPartition -> newMessage.offset))
    newAttempt should be(Some(newMessage.copy(offsets = manager.getOffsets)))
    oldAttempt should be(None)
  }

  it should "keep track across multiple partitions" in {
    // given
    val offsets = new OffsetManager[Array[Byte], Array[Byte]]
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
    val offsets = new OffsetManager[Array[Byte], Array[Byte]]
    val messageA: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val messageB: RxMessage = Message(value="test message".getBytes, topic="test-topic", partition=1, offset=0L)
    offsets.check(kafkaRawMessage(messageA))
    offsets.check(kafkaRawMessage(messageB))

    // when
    val partitions = offsets.getOffsets.keySet

    // then
    partitions should be(Set(messageA.topicPartition, messageB.topicPartition))
  }

  it should "adjust offsets for committing to zookeeper" in {
    // given an OffsetManager is in a certain state
    val offsets = new OffsetManager[Array[Byte], Array[Byte]]
    val topic = "test-topic"
    val messageA: RxMessage = Message(value="test message".getBytes, topic=topic, partition=0, offset=5L)
    val messageB: RxMessage = Message(value="test message".getBytes, topic=topic, partition=1, offset=20L)
    offsets.check(kafkaRawMessage(messageA))
    offsets.check(kafkaRawMessage(messageB))
    val higherOffset = messageA.offset+100
    val lowerOffset = messageB.offset-100

    // zookeeper partition A has a higher offset than our local offset manager is aware of
    // the only way we expect this to happen is another process is managing this partition
    // meaning we should relinquish ownership and filter out information on that partition
    val offsetsFromZK = Map(messageA.topicPartition -> higherOffset, messageB.topicPartition -> lowerOffset)

    // when
    val filteredOffsets = offsets.rebalanceOffsets(offsetsFromZK, offsets.getOffsets)

    // then
    // offsets need to be incremented for zookeeper:
    // kafka stored offsets are 'where do I start from' and our manager is 'what did I last process'
    val expectedManagerOffsets = Map(messageB.topicPartition -> messageB.offset)
    val expectedAdjustedOffsets = Map(messageB.topicPartition -> (messageB.offset + 1))

    offsets.getOffsets should be(expectedManagerOffsets)
    filteredOffsets should be(expectedAdjustedOffsets)
  }

  it should "provide its checkpoint function to all the messages" in {

    val failingFn = { (_: OffsetMap, _: OffsetMerge) =>
      throw new RuntimeException("This function should never be called")
      Map[TopicPartition, Long]()
    }

    val passingFn = { (_: OffsetMap, _ :OffsetMerge, _: OffsetMerge) =>
      "this function should pass".split(" ") should contain("pass")
      Map[TopicPartition, Long]()
    }

    val message: RxMessage = Message(value="test".getBytes, topic="test-topic", partition=0, offset=0L, mergeWith=failingFn)
    val manager = new OffsetManager[Array[Byte], Array[Byte]](commit=passingFn)

    val checkedMessage = manager.check(kafkaRawMessage(message))
    checkedMessage.get.commit() // should not call the failingFn, since the offset manager should have provided the passingFn
  }


  def kafkaRawMessage(message: RxMessage): KafkaMessage = {
    val msg = new kafka.message.Message(message.value)
    val decoder = new kafka.serializer.DefaultDecoder
    MessageAndMetadata(message.topic, message.partition, msg, message.offset, decoder, decoder)
  }


}
