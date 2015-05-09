import java.util.Properties

import com.cj.kafka.rx._
import scala.concurrent.duration._
import org.apache.kafka.clients.producer.{Producer,KafkaProducer}
import org.apache.kafka.common.serialization.ByteArraySerializer

object TopicTransformProducer extends App {

  type Key = Array[Byte]
  type Value = Array[Byte]
  type ByteProducer = Producer[Key, Value]
  type Result = ProducerResult[Array[Byte], Array[Byte]]
  
  val conn = new RxConnector("localhost:2181", "consume-and-produce-example")
  val topic = "words"
  val producer = getProducer

  conn.getMessageStream(topic)
    .map { message =>
      val WORD = new String(message.value).toUpperCase.getBytes("UTF-8")
      message.produce(WORD) 
    }
    .saveToKafka (producer, topic.toUpperCase)
    .tumblingBuffer (1.second, 10)
    .foreach { messages =>
      if (messages.nonEmpty) {
        messages.foreach(formatResult)
        messages.last.commit
      }
  }

  def formatResult(result: Result) = {
    val value = new String(result.value, "UTF-8")
    println(s"Produced: [${result.topic}] - ${result.partition} -> ${result.offset} :: ${value}")
  }

  def getProducer: ByteProducer = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("key.serializer", classOf[ByteArraySerializer].getCanonicalName)
    props.put("value.serializer", classOf[ByteArraySerializer].getCanonicalName)
    new KafkaProducer(props)
  }

}
