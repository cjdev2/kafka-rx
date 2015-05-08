import com.cj.kafka.rx.Message
import org.apache.kafka.clients.producer.MockProducer
import rx.lang.scala.Observable

import scala.collection.JavaConversions._

object KafkaProducerExample extends App {

    val producer = new MockProducer()

    //  conn.getMessageStream("xyz") map { message =>
    //    val value = new String(message.value)
    //    (message, value)
    //  } map { case (message, value) =>
    //    (message, value.toLowerCase)
    //  } filter { case (message, value) =>
    //      value.contains("/placement-t")
    //  } map { case (message, value) =>
    //      message.newCommitableProducerMessage("topic.name", value.getBytes("UTF-8"))
    //  } saveToKafka(producer)
    //
    //  conn.getMessageStream("xyz") map { message =>
    //      message.copy(value = new String(message.value, "UTF-8"))
    //  } filter { message =>
    //    message.value.toLowerCase.contains("/placement-t")
    //  } map { message =>
    //      message.copy(value = message.value.getBytes("UTF-8"))
    //  } map { message =>
    //      message.newCommitableProducerMessage("topic.name", message.value)
    //  } saveToKafka(producer)
    //
    //  val x: Observable[ProducerMessage[Nothing, Array[Byte]]] = conn.getMessageStream("xyz") map { message =>
    //    val producerMessage = message.newCommitableProducerMessage("topic.name", message.value)
    //    producerMessage.copy(value = new String(producerMessage.value, "UTF-8"))
    //  } filter { (message: ProducerMessage[Nothing, String]) =>
    //    message.value.toLowerCase.contains("/placement-t")
    //  } map { (message: ProducerMessage[Nothing, String]) =>
    //    message.copy(value = message.value.getBytes("UTF-8"))
    //  }

    val urls = Seq(
        "http://one/placement-t",
        "http://none/no-placement-t",
        "http://two/placement-t",
        "http://none/somewhere",
        "http://none/placement",
        "http://three/Placement-t"
    )

    val messages = urls.map(s => new Message[Array[Byte]](s.getBytes("UTF-8"), "", 0, 0, Map(), null))
    val stream = Observable.from(messages)

    val j = for {
        message <- stream
        if new String(message.value).toLowerCase.contains("/placement-t")
    } yield message.newCommittableProducerMessage("topic.name", message.value)

    j.saveToKafka(producer).foreach(println)

    j.foreach(onNext = {pr => println(new String(pr.value))})

    private val producerRecords = producer.history()

    producerRecords.foreach(println)


}