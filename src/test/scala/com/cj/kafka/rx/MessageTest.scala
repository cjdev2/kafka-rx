package com.cj.kafka.rx

import org.scalatest.{Matchers, FlatSpec}


class MessageTest extends FlatSpec with Matchers {

  "Message" should "be map to a new message with transformed value" in {
    //given
    val originalMessage = new Message("Key", 1, "topic", 0,0L)
    def fn(x: Int) = x + 1

    //when
    val newMessage = originalMessage.map(fn)

    //then
    newMessage.value should be (fn(originalMessage.value))
  }

}
