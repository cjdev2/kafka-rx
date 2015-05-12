import KafkaUtils._
import TwitterUtils._

import twitter4j._
import com.cj.kafka.rx._
import rx.lang.scala.Observable

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object TwitterStream {

  def main(args: Array[String]) = {
    Await.result(for {
      producer <- getProducerStream("tweets", args)
      consumer <- getConsumerStream("tweets")
    } yield {
      producer.merge(consumer)
    }, Duration.Inf) foreach (
      onNext = { msg => println(msg) },
      onError = { err => throw err }
      )
  }

  def getProducerStream(topic: String, topics: Array[String]): Future[Observable[String]] = Future {
    val kafka = getStringProducer(KAFKA)
    val twitter = getTwitter(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    val query = new FilterQuery().language(Array("en")).track(topics)
    getQueryStream(twitter, query) map { tweet: Status =>
      ProducerMessage(
        key = tweet.getUser.getScreenName,
        value = TwitterObjectFactory.getRawJSON(tweet)
      ).toProducerRecord(getTopic(tweet, topic, topics))
    } saveToKafka kafka map { message: Message[String, String] =>
      formatKafkaMessage(message)
    }
  }

  def getConsumerStream(topic: String): Future[Observable[String]] = Future {
    getStringStream(ZOOKEEPER, CONSUMER_GROUP, topic + ".*") map { message: Message[String, String] =>
      TwitterObjectFactory.createStatus(message.value)
    } map { tweet: Status =>
      formatTwitterStatus(tweet)
    }
  }

}