import com.cj.kafka.rx.RxConnector
import scala.concurrent.duration._

object WordCountConsumer extends App {

  // streaming word count, try it out with the kafka quickstart:
  // http://kafka.apache.org/documentation.html#quickstart

  val conn = new RxConnector("localhost:2181", "word-counter")

  val sentences = getStringStream("test")

  // if we split apart sentences we get words
  val words = sentences.flatMapIterable { sentence =>
    sentence.split("\\s+")
  }

  // if we collect words we can calculate frequencies
  val counts = words.scan(empty) { (counts, word) =>
    counts + (word -> (counts(word) + 1))
  }

  // sample results once a second instead of once per update
  counts.sample(1 second).foreach(println)

  def getStringStream(topic: String) =
    conn.getMessageStream(topic).map(_.value).map(new String(_))

  def empty = Map[String, Int]().withDefaultValue(0)

}