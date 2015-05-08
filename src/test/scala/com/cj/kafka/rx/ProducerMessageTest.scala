package com.cj.kafka.rx

import org.apache.kafka.clients.producer.{RecordMetadata, MockProducer}
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
            "http://three/Include-me"
        )

        val messages = urls.map(s => new Message[Array[Byte]](s.getBytes("UTF-8"), "", 0, 0, Map(), null))
        val stream = Observable.from(messages)
        val producer = new MockProducer()

        val j = stream.transform { m =>
            new ProducerMessage[Array[Byte], Array[Byte]](key = Some(m.value), value = Some(m.value))
        } filter { m =>
            new String(m.value.get).toLowerCase.contains("/include-me")
        }

        j.saveToKafka("topic.name")(producer).last.foreach { result =>
            result.commit
        }

        val history = producer.history()

        history.map { producerRecord =>
            producerRecord.topic()
        }.toSet shouldBe Set("topic.name")


        history.map { producerRecord =>
            new String(producerRecord.value(), "UTF-8")
        }.toSet shouldBe Set(
            "http://one/include-me",
            "http://two/include-me",
            "http://three/Include-me"
        )
    }
}
