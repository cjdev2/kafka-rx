package com.cj.kafka.rx

import org.apache.kafka.clients.producer.MockProducer
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}
import rx.lang.scala.Observable

import scala.collection.JavaConversions._

class ProducerMessageTest extends FlatSpec with Matchers with BeforeAndAfter {

    "ProducerMessage" should "should allow items to be save to a kafka stream" in {

        val urls = Seq(
            "http://one/include-me",
            "http://none/dont-include-me",
            "http://two/include-me",
            "http://none/include",
            "http://none/include",
            "http://three/include-me"
        )


        val messages = urls.map(s => new Message[Array[Byte]](s.getBytes("UTF-8"), "", 0, 0, Map(), null))
        val stream = Observable.from(messages)
        val producer = new MockProducer()

        val j = for {
            message <- stream
            if new String(message.value).toLowerCase.contains("/include-me")
        } yield message.newCommittableProducerMessage("topic.name", message.value)

        j.saveToKafka(producer)
          .foreach(println)

        val history = producer.history()

        history.map { producerRecord =>
            producerRecord.topic()
        }.toSet shouldBe Set("topic.name")


        history.map { producerRecord =>
            new String(producerRecord.value(), "UTF-8")
        }.toSet shouldBe Set(
            "http://one/include-me",
            "http://two/include-me",
            "http://three/include-me"
        )
    }
}
