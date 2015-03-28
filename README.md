# kafka-rx [![Build Status](https://travis-ci.org/cjdev/kafka-rx.svg)](https://travis-ci.org/cjdev/kafka-rx) [![Maven Central](https://img.shields.io/maven-central/v/com.cj/kafka-rx_2.10.svg)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.cj%22%20AND%20a%3A%22kafka-rx_2.10%22)

General Purpose Kafka Consumer that Just Behaves

#### Features

- thin adapter around kafka's high level api
- per message, fine grained commits semantics
- offset tracking for rebalancing and replay supression
- reactive api using rx-scala observables

#### Subscribing to a message stream:

kafka-rx provides a push alternative to kafka's pull-based iterable stream

To connect to your zookeeper cluster and process kafka streams:

```scala
val connector = new RxConnector("zookeeper:2181", "consumer-group")

connector.getObservableStream("cool-topic-(x|y|z)")
  .map(deserialize)
  .filter(interesting)
  .take(42 seconds)
  .foreach(println)

connector.shutdown()
```

#### Committing offset positions

kafka-rx was built with reliable message processing in mind

To support this, every kafka-rx message has a `.commit()` method which optionally takes a user provided merge function, giving the program an opportunity to reconcile with zookeeper and manage delivery guarantees.

```scala
stream.buffer(23).foreach { bucket =>
  bucket.last.commit { (zkOffsets, offsets) =>
    if (looksGood(zkOffsets)) offsets // go ahead and commit!
    else zkOffsets // or leave things as they were
    // or something else...
  }
}
```

If you can afford possible gaps in message processing you can also use kafka's automatic offset commit behavior, but you are encouraged to manage commits yourself.

#### Configuration

This and other consumer configuration can be provided through kafka's `ConsumerConfig`.

```scala
val conf = new ConsumerConfig(myProperties)
val conn = new RxConnector(conf)
```

#### Including in your project

Currently kafka-rx is built against kafka 0.8.2-beta and scala 2.10, but should work fine with other similar versions.

From maven:

```xml
<dependency>
  <groupId>com.cj</groupId>
  <artifactId>kafka-rx_2.11</artifactId>
  <version>0.1.1-SNAPSHOT</version>
</dependency>
```

From sbt:

```scala
libraryDependencies += "com.cj" % "kafka-rx_2.11" % "0.1.1-SNAPSHOT"
```

For more code and help getting started, check out the [examples](examples/).

#### Contributing

Have a question, improvement, or something you want to discuss?

Issues and pull requests welcome!

#### License

Eclipse Public License v.1 - Commission Junction 2015
