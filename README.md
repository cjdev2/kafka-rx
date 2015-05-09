# kafka-rx [![Build Status](https://travis-ci.org/cjdev/kafka-rx.svg)](https://travis-ci.org/cjdev/kafka-rx) [![Maven Central](https://img.shields.io/maven-central/v/com.cj/kafka-rx_2.10.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.cj%22%20AND%20a%3A%22kafka-rx_2.10%22)

General Purpose Kafka Client that Just Behaves

#### Features

- thin, reactive adapter around kafka's high level producer and consumer
- per message, fine grained commits semantics
- internal offsets management to keep track of consumers

#### Consuming messages:

kafka-rx provides a push alternative to kafka's pull-based iterable stream

To connect to your zookeeper cluster and process a stream:

```scala
val connector = new RxConnector("zookeeper:2181", "consumer-group")

connector.getMessageStream("cool-topic-(x|y|z)")
  .map(deserialize)
  .take(42 seconds)
  .foreach(println)

connector.shutdown()
```

All of the standard [rx transforms](http://rxmarbles.com/) are available on the resulting stream.

#### Producing messages

kafka-rx can also be used to produce kafka streams

```scala
tweetStream.map(parse)
  .groupBy(hashtag)
  .foreach { (tag, subStream) =>
    subStream.map(toProducerRecord)
      .saveToKafka(kafkaProducer, s"tweets.$tag")
      .foreach { savedMessage =>
        savedMessage.commit() // checkpoint position in the source stream
      }
  }
```

Check out the [words-to-WORDS](examples/TopicTransformProducer.scala) producer for a full working example.

#### Reliable Message Processing

kafka-rx was built with reliable message processing in mind

To support this, every kafka-rx message has a `.commit()` method which optionally takes a user provided merge function, giving the program an opportunity to reconcile with zookeeper and manage delivery guarantees.

```scala
stream.buffer(23).foreach { bucket =>
  process(bucket)
  bucket.last.commit()
}
```

If you can afford possible gaps in message processing you can also use kafka's automatic offset commit behavior, but you are encouraged to manage commits yourself.

In general you should aim for idempotent processing, where it is no different to process a message once or many times. In addition, remember that messages are delivered across different topic partitions in a non-deterministic order. If this is important you are encouraged to process each topic partition as an individual stream to ensure there is no interleaving.

```scala
val numStreams = numPartitions
val streams = conn.getMessageStreams(topic, numStreams)
```

#### Configuration

Wherever possible, kafka-rx delegates to kafka's internal configuration.

Use kafka's `ConsumerConfig` for configuring the consumer, and `ProducerConfig` for configuring your producer.

#### Including in your project

Currently kafka-rx is built against kafka 0.8.2.1 and scala 2.11, but should work fine with other similar versions.

From maven:

```xml
<dependency>
  <groupId>com.cj</groupId>
  <artifactId>kafka-rx_2.11</artifactId>
  <version>0.2.0-SNAPSHOT</version>
</dependency>
```

From sbt:

```scala
libraryDependencies += "com.cj" % "kafka-rx" % "0.2.0-SNAPSHOT"
```

#### Videos & Examples

For more code and help getting started, see the [examples](examples/).

Or, if videos are more your style:

[![stream processing with kafka-rx](http://img.youtube.com/vi/S-Ynyel9pkk/0.jpg)](http://www.youtube.com/watch?v=S-Ynyel9pkk)

#### Contributing

Have a question, improvement, or something you want to discuss?

Issues and pull requests welcome!

#### License

Eclipse Public License v.1 - Commission Junction 2015
