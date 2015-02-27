# kafka-rx [![Build Status](https://travis-ci.org/cjdev/kafka-rx.svg)](https://travis-ci.org/cjdev/kafka-rx)
General Purpose Kafka Consumer that Just Behaves

#### Features

- thin adapter around kafka's high level api
- per message, fine grained commits semantics
- offset tracking for rebalancing and replay supression
- push based api using rx-scala observables

#### Subscribing to a message stream:

```scala
val connector = new RxConnector("zookeeper:2181", "consumer-group")

connector.getObservableStream("my-topic")
  .map(deserialize)
  .filter(interesting)
  .take(30 seconds)
  .foreach(println)

connector.shutdown()
```

#### Committing offset positions

```scala
stream.buffer(500).foreach { bucket =>
  bucket.last.commit()
}
```
