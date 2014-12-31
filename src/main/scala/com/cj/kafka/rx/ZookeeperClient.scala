package com.cj.kafka.rx

import com.google.common.primitives.Longs
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.utils.ZKPaths
import scala.collection.JavaConversions._

import org.apache.curator.framework.recipes.locks.{InterProcessLock, InterProcessMutex}

class ZookeeperClient(topic: String, group: String, zk: CuratorFramework) {

  val offsetPath = ZKHelper.getConsumerOffsetPath(topic, group)

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
      ZKHelper.extractPartition(path) -> Longs.fromByteArray(bytes)
    }).toMap
  }

  def setOffsets(offsets: Map[Int, Long]): Map[Int, Long] = {
    offsets foreach { case (partition, offset) =>
      val nodePath = ZKHelper.getPartitionPath(offsetPath, partition)
      Option(zk.checkExists.forPath(nodePath)) match {
        case None =>
          zk.create.creatingParentsIfNeeded.forPath(nodePath, Longs.toByteArray(offset))
        case Some(fileStats) =>
          zk.setData().forPath(nodePath, Longs.toByteArray(offset))
      }
    }
    getOffsets
  }

  def getLock: InterProcessLock = {
    val lockPath = s"/locks/kafka/$topic.$group"
    new InterProcessMutex(zk, lockPath)
  }

}