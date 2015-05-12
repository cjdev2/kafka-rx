### Twitter Streaming Example

kafka-rx composes well with streaming apis

here we take the the [Twitter4J](http://twitter4j.org/en/index.html) streaming library and pour the events into kafka

To do this, we need to create an `Observable[ProducerRecord]` that represent the stream of messages we wish to produce. 

```scala
val stream = ...
stream.map(toProducerRecord).saveToKafka(kafka)
```

In [TwitterUtils.scala](src/main/scala/TwitterUtils.scala) we wrap the twitter4j api to return an `Observable[twitter4j.Status]` - and also a place to configure your [app credentials](https://apps.twitter.com/) - required to connect to the twitter api.

In [KafkaUtils.scala](src/main/scala/KafkaUtils.scala) we configure our kafka producers and consumers with string keys and string values, since we'll be storing the json provided by the twitter api.

Our entry point is [TwitterStream.scala](src/main/scala/TwitterStream.scala) where we create two threads, one to stream data out of twitter and into kafka, and another to stream data out of kafka and into our process.

Once you have configured your api credentials, try it out:

```
mvn scala:run -DaddArgs='computers|pizza|anime'
```

If you have kafka and zookeeper running locally, you should start seeing some tweets!

Don't worry if it takes a while for the consumer to catch up, the kafka consumer threads wake up every so often to look for new topics.

Try changing the query or the code!