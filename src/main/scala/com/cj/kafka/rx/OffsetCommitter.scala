package com.cj.kafka.rx

import com.google.common.base.Charsets
import com.google.common.primitives.Longs
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.utils.ZKPaths
import scala.collection.JavaConversions._

import org.apache.curator.framework.recipes.locks.{InterProcessLock, InterProcessMutex}

class OffsetCommitter(topic: String, group: String, zk: CuratorFramework) {

  val offsetPath = KafkaHelper.getConsumerOffsetPath(topic, group)

  def start() = {
    if (zk.getState != CuratorFrameworkState.STARTED) {
      zk.start()
      zk.blockUntilConnected()
    }
  }

  def close() = zk.close()

  def getOffsets: Map[Int, Long] = {
    val offsetPaths: Seq[String] =
      Option(zk.checkExists.forPath(offsetPath)) match {
        case None => List()
        case Some(fileStats) =>
          zk.getChildren.forPath(offsetPath)
            .map(ZKPaths.makePath(offsetPath, _))
      }
    offsetPaths.map({ path =>
      val bytes = zk.getData.forPath(path)
      val str = new String(bytes, Charsets.UTF_8).trim
      val offset = java.lang.Long.parseLong(str)
      KafkaHelper.extractPartition(path) -> offset
    }).toMap
  }

  def setOffsets(offsets: Map[Int, Long]): Map[Int, Long] = {
    offsets foreach { case (partition, offset) =>
      val nodePath = KafkaHelper.getPartitionPath(offsetPath, partition)
      val bytes = offset.toString.getBytes(Charsets.UTF_8)
      Option(zk.checkExists.forPath(nodePath)) match {
        case None =>
          zk.create.creatingParentsIfNeeded.forPath(nodePath, bytes)
        case Some(fileStats) =>
          zk.setData().forPath(nodePath, bytes)
      }
    }
    getOffsets
  }

  def getLock: InterProcessLock = {
    val lockPath = s"/locks/kafka-rx/$topic.$group"
    new InterProcessMutex(zk, lockPath)
  }

  def commit(manager: OffsetManager[Array[Byte]], offsets: Map[Int, Long], callback: (Map[Int, Long]) => Unit): Map[Int, Long] = {
    val lock = getLock
    lock.acquire()
    try {
      val zkOffsets = getOffsets
      callback(zkOffsets)
      val adjustedOffsets = manager.adjustOffsets(zkOffsets, offsets)
      setOffsets(adjustedOffsets)
    } catch {
      case err: Throwable =>
        err.printStackTrace()
        throw err
    } finally {
      lock.release()
    }
  }

}