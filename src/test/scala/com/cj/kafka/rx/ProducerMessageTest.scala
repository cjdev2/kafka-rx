package com.cj.kafka.rx

import org.apache.kafka.clients.producer.{RecordMetadata, MockProducer}
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}
import rx.lang.scala.Observable

import scala.collection.JavaConversions._

class ProducerMessageTest extends FlatSpec with Matchers with BeforeAndAfter {

    "ProducerMessage" should "should allow items to be saved to a kafka stream" in {

        val topicName = "topic.name"

        val urls = Seq(
            "http://one/include-me",
            "http://none/dont-include-me",
            "http://two/include-me",
            "http://none/include",
            "http://none/include",
            "http://three/Include-me"
        )

        val messages = urls.map(s => new Message(key = s.getBytes("UTF-8"), value = s.getBytes("UTF-8"), "", 0, 0))
        val stream = Observable.from(messages)
        val producer = new MockProducer()

        def pred(s: String) = s.toLowerCase.contains("/include-me")

        val metadataStream = stream.filter { m =>
            pred(new String(m.value, "UTF-8"))
        }.map { m =>
            m.produce(m.value, m.value)
        }.saveToKafka(producer, topicName)
            .toBlocking
            .toList

        val history = producer.history()

        history foreach { producerRecord =>
            producerRecord.topic shouldBe topicName
        }

        history.map { producerRecord =>
            new String(producerRecord.value(), "UTF-8")
        } shouldBe urls.filter(pred)
    }
}
