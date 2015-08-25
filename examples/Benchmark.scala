import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.producer.{Producer, ProducerRecord, ProducerConfig, KafkaProducer}
import rx.lang.scala.subjects.PublishSubject

import com.cj.kafka.rx._
import org.apache.kafka.common.serialization.ByteArraySerializer
import rx.lang.scala.Observable

import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.Random

import scala.concurrent.duration._

object InputStream extends App {
  val recordStream = util.bigStream map { x: BigInt =>
    new ProducerRecord(util.input, x.toInt % util.concurrency, x.toString().getBytes, util.randomBytes(100))
  }
  val saved = recordStream.saveToKafka(util.getProducer)
  util.countStream(saved) foreach println
}

object PipeStream extends App {

  val consumer = util.getConsumer
  val producer = util.getProducer

  val streams = consumer.getRecordStreams[Array[Byte], Array[Byte]](util.input, util.concurrency)

  val subject = PublishSubject[(Int, Long)]()
  val count = new AtomicLong()
  implicit val ec = util.getExecutionContext
  val counters = for (stream <- streams) yield Future {
    val id = count.incrementAndGet().toInt
    val recordStream = stream map { x => new ProducerRecord(util.output, x.key, x.value) }
    val saved = recordStream.saveToKafka(producer)
    util.countStream(saved).subscribe(msg => { subject.onNext(id -> msg) })
  }

  def sum = Future {
    val seed: Map[Int, Long] = Map()
    subject.scan(seed) { (accum, curr) =>
      accum + curr
    } map { counts =>
      counts.values.sum
    } sample(1 second) subscribe(msg => println(msg))
  }

  Await.result(Future.sequence(counters :+ sum), Duration.Inf)
}


object util {

  val concurrency = 2
  val input = "benchmark-input"
  val output = "benchmark-output"
  val group = "rx-benchmark-consumer"
  val kafka = "localhost:9092"
  val zookeeper = "localhost:2181"

  def getProducer: Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka)
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getCanonicalName)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def getConsumer: RxConsumer = {
    new RxConsumer(zookeeper, group, autocommit = true, startFromLatest = false)
  }

  def countStream(stream: Observable[Record[_, _]], time: Duration = 1.second): Observable[Long] = {
    for {
      slice <- stream.tumbling(time)
      count <- slice.countLong
    } yield count
  }

  def bigIterator(start: BigInt = 0, end: BigInt = BigInt("1000000000000"), step: BigInt = 1) = {
    Iterator.iterate(start)(_ + step).takeWhile(_ <= end)
  }

  def bigStream = Observable.from(new Iterable[BigInt] {
    def iterator: Iterator[BigInt] = bigIterator()
  })

  def getExecutionContext: ExecutionContext = {
    val context = Executors.newFixedThreadPool(util.concurrency + 2)
    ExecutionContext.fromExecutor(context)
  }

  val random = new Random()
  def randomBytes(amount: Int) = {
    val data = new Array[Byte](100)
    random.nextBytes(data)
    data
  }

}