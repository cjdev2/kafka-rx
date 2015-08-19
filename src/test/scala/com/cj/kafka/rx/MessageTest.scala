package com.cj.kafka.rx

import org.scalatest.{Matchers, FlatSpec}


class MessageTest extends FlatSpec with Matchers {

  val originalMessage = new Message("Key", 1, "topic", 0,0L)
  def transform(x: Int)  = x + 1


  "Message" should "be map to a new message with transformed value" in {
    //given

    //when
    val newMessage = originalMessage.map(transform)

    //then
    newMessage.value should be (transform(originalMessage.value))
  }

  "Message" should "keep original key" in {

    // given
    // (setup)

    // when
    val newMessage = originalMessage.map(transform)

    // then
    newMessage.key should be(originalMessage.key)

  }

  "Message" should "keep original topic" in {
    true should be("not yet implemented")
  }


}
