import java.util.Properties

import com.cj.kafka.rx._
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
        key = message.value.toUpperCase,
        value = message.value.toUpperCase
      )
    }
    .saveToKafka(getProducer, topic.toUpperCase)
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

  def getProducer: StringProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer(props)
  }

  def getStringStream(conn: RxConnector, topic: String) = {
    conn.getMessageStream(topic).map { message =>
      message.copy(value = new String(message.value, "UTF-8"))
    }
  }

}
