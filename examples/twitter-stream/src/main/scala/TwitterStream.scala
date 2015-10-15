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
    await[Observable[String]] {
      for {
        producer <- getProducerStream(KAFKA_TWITTER_TOPIC_PREFIX, args)
        consumer <- getConsumerStream(KAFKA_TWITTER_TOPIC_PREFIX)
      } yield producer.merge(consumer)
    } foreach (
      onNext = { msg => println(msg) },
      onError = { err => throw err }
    )
  }

  def getProducerStream(namespace: String, topics: Array[String]): Future[Observable[String]] = Future {
    val kafkaProducer = getStringProducer(KAFKA)
    val twitter = getTwitter(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    val query = new FilterQuery().language(Array("en")).track(topics)
    getQueryStream(twitter, query) groupBy { tweet: Status =>
      getTopic(tweet, namespace, topics)
    } flatMap { case (topic, subStream) =>
      subStream
        .map(toProducerRecord(topic, _))
        .saveToKafka(kafkaProducer)
    } map { record: Record[String, String] =>
      formatKafkaRecord(record)
    }
  }

  def getConsumerStream(topic: String): Future[Observable[String]] = Future {
    val topicWildcard = topic + ".*"
    getStringStream(ZOOKEEPER, CONSUMER_GROUP, topicWildcard) map { record: Record[String, String] =>
      TwitterObjectFactory.createStatus(record.value)
    } map { tweet: Status =>
      formatTwitterStatus(tweet)
    }
  }

  def await[T](future: Future[T]) = Await.result(future, Duration.Inf)

}