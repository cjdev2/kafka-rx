import com.cj.kafka.rx.{OffsetMap, OffsetMerge, ProducerMessage, RxConnector}
import org.apache.kafka.clients.producer.{MockProducer, Producer, RecordMetadata}
import rx.lang.scala.Observable

object TopicTransformProducer extends App {

  val conn = new RxConnector("localhost:2181", "consume-and-produce-example")
  implicit val producer: Producer = new MockProducer()

  val messages: Observable[ProducerMessage[Array[Byte], Array[Byte]]] =
    conn.getMessageStream("source.topic") transform {
    message => {
      ProducerMessage(key = Some(message.value), value = Some(new String(message.value).toUpperCase.getBytes("UTF-8")), commitFn = Some(message.commit _))
    }
  }

  messages saveToKafka "target.topic" tumbling 150 foreach {
    (messages: Observable[(RecordMetadata, (OffsetMerge) => OffsetMap)]) =>
      messages.last.foreach { case (metadata, commit) => commit}
  }


}
