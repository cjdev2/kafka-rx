import com.cj.kafka.rx.RxConnector
import scala.concurrent.duration._

object WordCountConsumer extends App {

  val conn = new RxConnector("zookeeper:2181", "consumer-group")

  // our empty word count type
  val empty = Map[String, Int]()

  val sentences = getStringStream("sentences")

  // if we split apart sentences we get words
  val words = sentences.flatMapIterable(_.split("\\s+"))

  // if we collect words we can calculate frequencies
  val wordCounts = words.foldLeft(empty) { (counts, word) =>
    counts.updated(word, counts.getOrElse(word, 0) + 1)
  }

  // sample once a second instead of once per update
  wordCounts.sample(1 second).foreach(println)

  def getStringStream(topic: String) = {
    conn.getMessageStream(topic).map(_.value).map(new String(_))
  }

}