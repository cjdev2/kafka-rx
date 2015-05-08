import com.cj.kafka.rx.{OffsetMap, OffsetMerge, ProducerMessage, RxConnector}
import org.apache.kafka.clients.producer.{MockProducer, Producer, RecordMetadata}
import rx.lang.scala.Observable

object TopicTransformProducer extends App {

  val conn = new RxConnector("localhost:2181", "consume-and-produce-example")
  implicit val producer = new MockProducer()

  val messages: Observable[ProducerMessage[Array[Byte], Array[Byte]]] =
    conn.getMessageStream(".*") transform {
    message => {
      ProducerMessage(key = Some(message.value), value = Some(new String(message.value).toUpperCase.getBytes("UTF-8")), commitFn = Some(message.commit _))
    }
  }

  messages saveToKafka "target.topic" tumbling 150 foreach { messages =>
      messages.last.foreach { result => result.commit }
  }
}
