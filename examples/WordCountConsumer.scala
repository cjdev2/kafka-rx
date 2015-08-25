import com.cj.kafka.rx.RxConsumer
import kafka.serializer.StringDecoder
import scala.concurrent.duration._

object WordCountConsumer extends App {

  // streaming word count, try it out with the kafka quickstart:
  // http://kafka.apache.org/documentation.html#quickstart

  val conn = new RxConsumer("localhost:2181", "word-counter")

  val decoder = new StringDecoder
  val sentences = conn.getRecordStream("test", keyDecoder = decoder, valueDecoder = decoder)

  // if we split apart sentences we get words
  val words = sentences.flatMapIterable { message =>
    message.value.split("\\s+")
  }

  // if we collect words we can calculate frequencies
  val counts = words.scan(empty) { (counts, word) =>
    counts + (word -> (counts(word) + 1))
  }

  // sample results once a second instead of once per update
  counts.sample(1 second).foreach(println)

  def empty = Map[String, Int]().withDefaultValue(0)

}