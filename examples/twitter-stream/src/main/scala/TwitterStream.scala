import com.cj.kafka.rx._
import kafka.serializer.StringDecoder
import twitter4j._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// utilities for connecting to kafka & twitter
import KafkaUtils._
import TwitterUtils._

object TwitterStream extends App {

  val twitter = getTwitterStream(
    "CONSUMER_KEY",
    "CONSUMER_SECRET",
    "ACCESS_TOKEN",
    "ACCESS_SECRET"
  )

  val topics = List(
    "cats",
    "dogs",
    "stream processing"
  )

  val threads = Future.sequence(Seq(
    // producer thread will read tweets from the gardenhose and save them to kafka
    Future {
      val query = new FilterQuery().track(topics.toArray)
      getFilterStream(twitter, query) map { tweet =>
        ProducerMessage(
          key = tweet.getId.toString,
          value = TwitterObjectFactory.getRawJSON(tweet)
        )
      } saveToKafka(getStringProducer, "tweets") foreach { message =>
        println(formatMessage(message))
      }
    },

    // consumer thread will read messages from kafka, and decode them into tweets
    Future {
      val decoder = new StringDecoder()
      val config = new SimpleConfig("localhost:2181", "tweet-consumer", true, true)
      val stream = new RxConnector(config)
        .getMessageStream("tweets", keyDecoder = decoder, valueDecoder = decoder)
        .map { message => TwitterObjectFactory.createStatus(message.value) }
        .foreach { status => println(formatTweet(status)) }
    }
  ))

  Await.result(threads, 1 hour)

}