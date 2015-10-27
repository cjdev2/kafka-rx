import java.io.FileInputStream
import java.util.Properties

import com.cj.kafka.rx.{Record, RxConsumer}
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.kafka.common.serialization.StringSerializer
import rx.lang.scala.Observable

object KafkaUtils {

  val props = new Properties()
  val input = new FileInputStream("src/main/resources/kafka.properties")
  props.load(input)
  input.close()

  val KAFKA          = props.getProperty("kafka.brokers")
  val ZOOKEEPER      = props.getProperty("kafka.zkQuorum")
  val CONSUMER_GROUP = props.getProperty("kafka.twitter_consumer_groupid")
  val KAFKA_TWITTER_TOPIC_PREFIX = props.getProperty("kafka.twitter_topic_prefix")

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
