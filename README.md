# kafka-rx [![Build Status](https://travis-ci.org/cjdev/kafka-rx.svg)](https://travis-ci.org/cjdev/kafka-rx) [![Maven Central](https://img.shields.io/maven-central/v/com.cj/kafka-rx_2.11.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.cj%22%20AND%20a%3A%22kafka-rx_2.11%22)

[![Join the chat at https://gitter.im/cjdev/kafka-rx](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cjdev/kafka-rx?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

General Purpose [Kafka](https://kafka.apache.org) Client that Just Behaves

#### Features

- thin, reactive adapter around kafka's producer and consumer
- per record, fine grained commits semantics
- offset management to keep track of consumer positions

#### Consuming records:

kafka-rx provides a push alternative to kafka's pull-based stream

To connect to your zookeeper cluster and process a stream:

```scala
val consumer = new RxConsumer("zookeeper:2181", "consumer-group")

consumer.getRecordStream("cool-topic-(x|y|z)")
  .map(deserialize)
  .take(42 seconds)
  .foreach(println)

consumer.shutdown()
```

All of the standard [rx transforms](http://rxmarbles.com/) are available on the resulting stream.

#### Producing records

kafka-rx can also be used to produce kafka streams

```scala
tweetStream.map(parse)
  .groupBy(hashtag)
  .foreach { (tag, subStream) =>
    subStream.map(toProducerRecord)
      .saveToKafka(kafkaProducer)
      .foreach { savedRecord =>
        savedRecord.commit() // checkpoint position in the source stream
      }
  }
```

Check out the [words-to-WORDS](examples/TopicTransformProducer.scala) producer or the [twitter-stream](examples/twitter-stream) demo for a full working example.

#### Reliable Record Processing

kafka-rx was built with reliable record processing in mind

To support this, every kafka-rx record has a `.commit()` method which optionally takes a user provided merge function, giving the program an opportunity to reconcile offsets with zookeeper and manage delivery guarantees.

```scala
stream.buffer(23).foreach { bucket =>
  process(bucket)
  bucket.last.commit()
}
```

If you can afford possible gaps in record processing you can also use kafka's automatic offset commit behavior, but you are encouraged to manage commits yourself.

In general you should aim for idempotent processing, where it is no different to process a record once or many times. In addition, remember that records are delivered across different topic partitions in a non-deterministic order. If this is important you are encouraged to process each topic partition as an individual stream to ensure there is no interleaving.

```scala
val numStreams = numPartitions
val streams = consumer.getRecordStreams(topic, numStreams)
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
  <version>0.3.0</version>
</dependency>
```

From sbt:

```scala
libraryDependencies += "com.cj" % "kafka-rx" % "0.3.0"
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
