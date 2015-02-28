import java.io.{FileOutputStream, File}

import com.cj.kafka.rx.RxConnector
import scala.concurrent.duration._

object FileWriterConsumer extends App {

  val conn = new RxConnector("zookeeper:2181", "consumer-group")

  // kafka rx job to write messages on all topics to disk
  conn.getMessageStream(".*")
    // we can use groupBy to partition output by topic
    .groupBy(_.topicPartition)
    // for each topic / partition pair we get a new stream of events
    .foreach { case (topicPartition, stream) =>
      stream
        // we can create smaller chunks of these streams by time or size
        .tumblingBuffer(5 seconds)
        // and process them as micro-batches of messages
        .foreach { messages =>
          if (messages.nonEmpty) {
            // and simply write our file
            val file = getFile(topicPartition, messages.head.offset)
            for (message <- messages) writeToFile(file, message.value, appending=true)
            messages.last.commit()
          }
      }
    }

  def getFile(topicPartition: (String, Int), offset: Long) = {
    val (topic, partition) = topicPartition
    val (dir, name) = (s"target/$topic", s"$partition.$offset.log")
    new File(dir).mkdirs()
    new File(dir, name)
  }

  def writeToFile(file: File, data: Array[Byte], appending: Boolean = false) = {
    val fos = new FileOutputStream(file, appending)
    try fos.write(data)
    finally fos.close()
  }

}