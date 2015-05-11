import twitter4j._
import com.cj.kafka.rx._
import rx.lang.scala.Observable

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object TwitterStream extends App {

  import Helpers._

  val KAFKA = "localhost:9091"
  val ZOOKEEPER = "localhost:2181"
  
  val CONSUMER_KEY = "CONSUMER_KEY"
  val CONSUMER_SECRET = "CONSUMER_SECRET"
  val ACCESS_TOKEN = "ACCESS_TOKEN"
  val ACCESS_SECRET = "ACCESS_SECRET"

  getResultStream(KAFKA, ZOOKEEPER, "tweets") foreach { result =>
    println(result)
  }

  def getResultStream(brokers: String, zookeepers: String, topic: String): Observable[String] = {
    val results = for {
      producer <- getProducerStream(brokers, topic)
      consumer <- getConsumerStream(zookeepers, topic)
    } yield {
      producer.merge(consumer)
    }
    Await.result(results, 30 seconds)
  }

  def getProducerStream(brokers: String, kafkaTopic: String): Future[Observable[String]] = Future {
    val kafka = getKafkaProducer(brokers)
    val twitter = getTwitter(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN,ACCESS_SECRET)
    val topics = List("cats", "dogs", "stream processing")
    getTweetStream(twitter, topics) map { tweet =>
      ProducerMessage(
        key = tweet.getId.toString,
        value = TwitterObjectFactory.getRawJSON(tweet)
      )
    } saveToKafka(kafka, kafkaTopic) map { message =>
      formatMessage(message)
    }
  }

  def getConsumerStream(zookeepers: String, kafkaTopic: String): Future[Observable[String]] = Future {
    getMessageStream(zookeepers, kafkaTopic) map { message =>
      TwitterObjectFactory.createStatus(message.value)
    } map { status =>
      formatTweet(status)
    }
  }

}

