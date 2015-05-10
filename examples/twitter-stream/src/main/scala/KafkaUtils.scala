import java.util.Properties

import com.cj.kafka.rx.Message
import org.apache.kafka.clients.producer.{Producer, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaUtils {
  def getStringProducer: Producer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9091")
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }

  def formatMessage(message: Message[_, _]) = {
    s"put offset ${message.offset} @ ${message.topic}->${message.partition}"
  }
}
