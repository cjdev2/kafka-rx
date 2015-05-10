import rx.lang.scala.subjects.PublishSubject
import twitter4j._

object TwitterUtils {
  def getAuthentication(consumerKey: String, consumerSecret: String, accessToken: String, accessSecret: String) = {
    new twitter4j.conf.ConfigurationBuilder()
      .setJSONStoreEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessSecret)
      .build
  }

  def getFilterStream(stream: TwitterStream, query: FilterQuery) = {
    val subject = PublishSubject[Status]()
    stream.addListener(new StatusListener() {
      def onStatus(status: Status) { subject.onNext(status) }
      def onException(ex: Exception) { subject.onError(ex) }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onScrubGeo(arg0: Long, arg1: Long) {}
      def onStallWarning(warning: StallWarning) {}
    })
    stream.filter(query)
    subject
  }

  def getTwitterStream(consumerKey: String, consumerSecret: String, accessToken: String, accessSecret: String) = {
    new TwitterStreamFactory(getAuthentication(consumerKey, consumerSecret, accessToken, accessSecret)).getInstance
  }

  def formatTweet(status: Status) = s"<${status.getUser.getName}> ${status.getText.replaceAll("\n", "")}"

}