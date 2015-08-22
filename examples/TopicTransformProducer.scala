import java.util.Properties

import com.cj.kafka.rx._
import kafka.serializer.StringDecoder
import scala.concurrent.duration._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{Producer,KafkaProducer}

object TopicTransformProducer extends App {

  type Key = String
  type Value = String
  type StringProducer = Producer[Key, Value]

  val conn = new RxConnector("localhost:2181", "words-to-WORDS")
  val topic = "words"

  getStringStream(conn, topic)
    .map { message =>
      message.produce(
        topic = topic.toUpperCase,
        key = message.key,
        value = message.value.toUpperCase
      )
    }
    .saveToKafka(getProducer)
    .tumblingBuffer(1.second, 10)
    .foreach { messages =>
      if (messages.nonEmpty) {
        messages.foreach(formatMessage)
        messages.last.commit()
      }
  }

  def formatMessage(result: Message[Key, Value]) = {
    println(s"Produced: [${result.topic}] - ${result.partition} -> ${result.offset} :: ${result.value}")
  }

  def getStringStream(conn: RxConnector, topic: String) = {
    conn.getMessageStream[Key, Value](topic, keyDecoder = new StringDecoder, valueDecoder = new StringDecoder)
  }

  def getProducer: StringProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[Key, Value](props)
  }


}
