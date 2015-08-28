import java.util.Properties

import com.cj.kafka.rx.{Record, RxConsumer}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.kafka.common.serialization.StringSerializer
import rx.lang.scala.Observable

object KafkaUtils {

  val KAFKA = "localhost:9091"
  val ZOOKEEPER = "localhost:2181"

  val CONSUMER_GROUP = "twitter-consumer"

  type Key = String
  type Value = String

  def getStringStream(zookeepers: String, group: String, kafkaTopic: String): Observable[Record[String, String]] = {
    new RxConsumer(
      zookeepers = zookeepers,
      group = group,
      autocommit = true,
      startFromLatest = true
    ).getRecordStream(
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

  def formatKafkaRecord(record: Record[Key, Value]) = {
    s"[kafka] ${record.topic} => ${record.partition} => ${record.offset}"
  }

}
