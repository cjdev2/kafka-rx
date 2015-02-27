# kafka-rx [![Build Status](https://travis-ci.org/cjdev/kafka-rx.svg)](https://travis-ci.org/cjdev/kafka-rx)
Horizontally Scalable, General Purpose Kafka Consumer that Just Behaves

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
