import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import twitter4j._
import com.cj.kafka.rx._

object Helpers {

  // kafka utils
  // establish connections to the cluster

  def getKafkaProducer(kafka: String): Producer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", kafka)
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }

  def getMessageStream(zookeepers: String, topic: String): Observable[Message[String, String]] = {
    val decoder = new StringDecoder()
    val config = new SimpleConfig(zookeepers, "tweet-consumer", true, true)
    new RxConnector(config).getMessageStream(topic, keyDecoder = decoder, valueDecoder = decoder)
  }

  // twitter utils
  // transform the twitter4j stream into an Observable[Status] stream
  
  def getTweetStream(twitter: TwitterStream, query: Seq[String]): Observable[Status] = {
    val filter = new FilterQuery().track(query.toArray)
    getFilterStream(twitter, filter)
  }

  def getTwitter(consumerKey: String, consumerSecret: String, accessToken: String, accessSecret: String): TwitterStream = {
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessSecret)
      .build
    new TwitterStreamFactory(config).getInstance
  }

  def getFilterStream(twitter: TwitterStream, query: FilterQuery): Observable[Status] = {
    val subject = PublishSubject[Status]()
    twitter.addListener(new StatusListener() {
      def onStatus(status: Status) { subject.onNext(status) }
      def onException(ex: Exception) { subject.onError(ex) }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    })
    twitter.filter(query)
    subject
  }

  // formatters
  // kafka rx messages and twitter4j statuses

  def formatMessage(message: Message[String, String]): String  = {
    s"put offset ${message.offset} @ ${message.topic}->${message.partition}"
  }

  def formatTweet(status: Status): String = {
    s"${status.getUser.getName} says: ${status.getText.replaceAll("\n", "")}"
  }

}
