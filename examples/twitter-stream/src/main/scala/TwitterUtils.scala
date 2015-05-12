import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import twitter4j._

object TwitterUtils {

  // twitter app credentials:
  // https://apps.twitter.com/

  val CONSUMER_KEY = "CONSUMER_KEY"
  val CONSUMER_SECRET = "CONSUMER_SECRET"
  val ACCESS_TOKEN = "ACCESS_TOKEN"
  val ACCESS_SECRET = "ACCESS_SECRET"

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

  def getQueryStream(stream: TwitterStream, query: FilterQuery): Observable[Status] = {
    val subject = PublishSubject[Status]()
    stream.addListener(new StatusListener() {
      def onStatus(status: Status) {
        subject.onNext(status)
      }
      def onException(ex: Exception) {
        subject.onError(ex)
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    })
    stream.filter(query)
    subject
  }

  def getTopic(tweet: Status, topic: String, topics: Seq[String]) = {
    s"$topic.${topics.find(tweet.getText.toLowerCase.contains(_)).getOrElse("unknown").replaceAll("\\s+", "_")}"
  }

  def formatTwitterStatus(status: Status) = {
    s"<${status.getUser.getScreenName}> ${status.getText.replaceAll("\n", "")}"
  }

}