package com.cj.kafka.rx

import KafkaHelper._

import org.scalatest.{FlatSpec, ShouldMatchers}

class OffsetManagerTest extends FlatSpec with ShouldMatchers {

  "OffsetManager" should "begins with no offsets" in {
    val offsets = new OffsetManager
    offsets.getOffsets should be(Map[Int, Long]())
  }

  it should "record offsets for kafka messages" in {
    // given
    val offsets = new OffsetManager
    val message = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)

    // when
    offsets.check(message.kafkaMessage)

    // then
    offsets.getOffsets should be(Map(message.topicPartition -> message.offset))
  }

  it should "not record the same message twice" in {
    // given
    val manager = new OffsetManager
    val message = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)

    // when
    val firstAttempt = manager.check(message.kafkaMessage)
    val secondAttempt = manager.check(message.kafkaMessage)

    // then
    firstAttempt should be(Some(message.copy(offsets = manager.getOffsets)))
    secondAttempt should be(None)
  }

  it should "not update offsets for old messages" in {
    // given
    val manager = new OffsetManager
    val oldMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val newMessage = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=1L)

    // when
    val newAttempt = manager.check(newMessage.kafkaMessage)
    val oldAttempt = manager.check(oldMessage.kafkaMessage)

    // then
    manager.getOffsets should be(Map(newMessage.topicPartition -> newMessage.offset))
    newAttempt should be(Some(newMessage.copy(offsets = manager.getOffsets)))
    oldAttempt should be(None)
  }

  it should "keep track across multiple partitions" in {
    // given
    val offsets = new OffsetManager
    val messageA = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val messageB = Message(value="test message".getBytes, topic="test-topic", partition=1, offset=0L)

    // when
    offsets.check(messageA.kafkaMessage)
    offsets.check(messageB.kafkaMessage)

    // then
    offsets.getOffsets should be(Map(messageA.topicPartition -> messageA.offset, messageB.topicPartition -> messageB.offset))
  }

  it should "know which partitions it owns" in {
    // given
    val offsets = new OffsetManager
    val messageA = Message(value="test message".getBytes, topic="test-topic", partition=0, offset=0L)
    val messageB = Message(value="test message".getBytes, topic="test-topic", partition=1, offset=0L)
    offsets.check(messageA.kafkaMessage)
    offsets.check(messageB.kafkaMessage)

    // when
    val partitions = offsets.getOffsets.keySet

    // then
    partitions should be(Set(messageA.topicPartition, messageB.topicPartition))
  }

  it should "adjust offsets for committing to zookeeper" in {
    // given an OffsetManager is in a certain state
    val offsetManager = new OffsetManager
    val topic = "test-topic"
    val messageA = Message(value="test message".getBytes, topic=topic, partition=0, offset=5L)
    val messageB = Message(value="test message".getBytes, topic=topic, partition=1, offset=20L)
    offsetManager.check(messageA.kafkaMessage)
    offsetManager.check(messageB.kafkaMessage)
    val higherOffset = messageA.offset+100
    val lowerOffset = messageB.offset-100

    // zookeeper partition A has a higher offset than our local offset manager is aware of
    // the only way we expect this to happen is another process is managing this partition
    // meaning we should relinquish ownership and filter out information on that partition
    val offsetsFromZK = Map(messageA.topicPartition -> higherOffset, messageB.topicPartition -> lowerOffset)

    // when
    val filteredOffsets = offsetManager.rebalanceOffsets(offsetsFromZK, offsetManager.getOffsets)

    // then
    // offsets need to be incremented for zookeeper:
    // kafka stored offsets are 'where do I start from' and our manager is 'what did I last process'
    val expectedManagerOffsets = Map(messageB.topicPartition -> messageB.offset)
    val expectedAdjustedOffsets = Map(messageB.topicPartition -> (messageB.offset + 1))

    offsetManager.getOffsets should be(expectedManagerOffsets)
    filteredOffsets should be(expectedAdjustedOffsets)
  }

  it should "provide its checkpoint function to all the messages" in {

    val failingFn = { (_: OffsetMap, _: Correction) =>
      throw new RuntimeException("This function should never be called")
      Map[TopicPartition, Long]()
    }

    val passingFn = { (_: OffsetMap, _ :Correction, _: Rebalance) =>
      "this function should pass".split(" ") should contain("pass")
      Map[TopicPartition, Long]()
    }

    val message = Message(value="test".getBytes, topic="test-topic", partition=0, offset=0L, checkpointWith=failingFn)
    val manager = new OffsetManager(commit=passingFn)

    val checkedMessage = manager.check(message.kafkaMessage)
    checkedMessage.get.checkpoint() // should not call the failingFn, since the offset manager should have provided the passingFn
  }

}
