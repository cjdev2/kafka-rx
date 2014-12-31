package com.cj.kafka.rx

import org.scalatest.{FlatSpec, ShouldMatchers}

class OffsetManagerTest extends FlatSpec with ShouldMatchers {

  "OffsetManager" should "begins with no offsets" in {
    val offsets = new OffsetManager[String]
    offsets.getOffsets should be(Map[Int, Long]())
  }

  it should "record offsets for kafka messages" in {
    // given
    val offsets = new OffsetManager[String]
    val message = Message(value="test message", topic="test-topic", partition=0, offset=0L)

    // when
    offsets.check(message)

    // then
    offsets.getOffsets should be(Map(message.partition -> message.offset))
  }

  it should "not record the same message twice" in {
    // given
    val offsets = new OffsetManager[String]
    val message = Message(value="test message", topic="test-topic", partition=0, offset=0L)

    // when
    val firstAttempt = offsets.check(message)
    val secondAttempt = offsets.check(message)

    // then
    firstAttempt should be(Some(message.copy(offsets = offsets.getOffsets)))
    secondAttempt should be(None)
  }

  it should "not update offsets for old messages" in {
    // given
    val offsets = new OffsetManager[String]
    val oldMessage = Message(value="test message", topic="test-topic", partition=0, offset=0L)
    val newMessage = Message(value="test message", topic="test-topic", partition=0, offset=1L)

    // when
    val newAttempt = offsets.check(newMessage)
    val oldAttempt = offsets.check(oldMessage)

    // then
    offsets.getOffsets should be(Map(newMessage.partition -> newMessage.offset))
    newAttempt should be(Some(newMessage.copy(offsets = offsets.getOffsets)))
    oldAttempt should be(None)
  }

  it should "keep track across multiple partitions" in {
    // given
    val offsets = new OffsetManager[String]
    val messageA = Message(value="test message", topic="test-topic", partition=0, offset=0L)
    val messageB = Message(value="test message", topic="test-topic", partition=1, offset=0L)

    // when
    offsets.check(messageA)
    offsets.check(messageB)

    // then
    offsets.getOffsets should be(Map(messageA.partition -> messageA.offset, messageB.partition -> messageB.offset))
  }

  it should "know which partitions it owns" in {
    // given
    val offsets = new OffsetManager[String]
    val messageA = Message(value="test message", topic="test-topic", partition=0, offset=0L)
    val messageB = Message(value="test message", topic="test-topic", partition=1, offset=0L)
    offsets.check(messageA)
    offsets.check(messageB)

    // when
    val partitions = offsets.getOffsets.keySet

    // then
    partitions should be(Set(messageA.partition, messageB.partition))
  }

  it should "reconcile partition ownership with zookeepers external offsets" in {
    // given an OffsetManager is in a certain state
    val offsetManager = new OffsetManager[String]
    val messageA = Message(value="test message", topic="test-topic", partition=0, offset=5L)
    val messageB = Message(value="test message", topic="test-topic", partition=1, offset=20L)
    offsetManager.check(messageA)
    offsetManager.check(messageB)
    val higherOffset = messageA.offset+100
    val lowerOffset = messageB.offset-100

    // zookeeper partition A has a higher offset than our local offset manager is aware of
    // the only way we expect this to happen is another process is managing this partition
    // meaning we should relinquish ownership and filter out information on that partition
    val offsetsFromZK = Map(messageA.partition -> higherOffset, messageB.partition -> lowerOffset)

    // when
    val filteredOffsets = offsetManager.adjustOffsets(offsetsFromZK)

    // then
    val expectedOffsets = Map(messageB.partition -> messageB.offset)
    offsetManager.getOffsets should be(expectedOffsets)
    filteredOffsets should be(expectedOffsets)
  }

  it should "provide its checkpoint function to all the messages" in {

    val failingFn = { x: Map[Int, Long] =>
      throw new RuntimeException("This function should never be called")
      Map[Int, Long]()
    }

    val passingFn = { (_: OffsetManager[String], _: Map[Int, Long]) =>
      "this function should pass".split(" ") should contain("pass")
      Map[Int, Long]()
    }

    val message = Message(value="test", topic="test-topic", partition=0, offset=0L, checkpointFn=failingFn)
    val manager = new OffsetManager[String](commit=passingFn)

    val checkedMessage = manager.check(message)
    checkedMessage.get.checkpoint() // should not call the failingFn, since the offset manager should have provided the passingFn
  }

}
