import java.util.Properties

import com.cj.kafka.rx.{RxConnector, Message}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{Producer, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer
import rx.lang.scala.Observable

object KafkaUtils {

  val KAFKA = "localhost:9091"
  val ZOOKEEPER = "localhost:2181"

  val CONSUMER_GROUP = "twitter-consumer"

  type Key = String
  type Value = String

  def getStringStream(zookeepers: String, group: String, kafkaTopic: String): Observable[Message[Key, Value]] = {
    new RxConnector(
      zookeepers = zookeepers,
      group = group,
      autocommit = true,
      startFromLatest = true
    ).getMessageStream(
      topic = kafkaTopic,
      keyDecoder = new StringDecoder,
      valueDecoder = new StringDecoder
    )
  }

  def getStringProducer(servers: String): Producer[Key, Value] = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("producer.type", "async")
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }

  def formatKafkaMessage(message: Message[Key, Value]) = {
    s"[kafka] ${message.topic} => ${message.partition} => ${message.offset}"
  }

}
