package com.cj.kafka.rx

import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import rx.lang.scala.Observable

import scala.collection.JavaConversions._

class ProducerRecordTest extends FlatSpec with Matchers with BeforeAndAfter {

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

        val messages = urls.map(s => new Record(key = s.getBytes("UTF-8"), value = s.getBytes("UTF-8"), "", 0, 0))
        val stream = Observable.from(messages)
        val producer = new MockProducer(true, new ByteArraySerializer(), new ByteArraySerializer())

        def pred(s: String) = s.toLowerCase.contains("/include-me")

        val metadataStream = stream.filter { m =>
            pred(new String(m.value, "UTF-8"))
        }.map { m =>
            m.produce(topicName, m.value, m.value)
        }.saveToKafka(producer)
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

    it should "maintain commit context when used with kafka producers" in {
        var commitWasCalled = false
        val stream: Observable[Committable[ProducerRecord[Array[Byte], Array[Byte]]]] = Observable.just(
            new Record("key".getBytes, "val".getBytes, "topic", 0, 0, commitfn = { _ =>
                commitWasCalled = true; Map()
            })
        ) map { x =>
            x.produce("lol")
        }
        val fakeProducer = new MockProducer(true, new ByteArraySerializer(), new ByteArraySerializer())
        val result = stream.saveToKafka(fakeProducer).toBlocking.toList

        result.last.commit()
        commitWasCalled should be (true)
    }

}
