package com.cj.kafka.rx

import com.google.common.base.Charsets
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.locks.{InterProcessLock, InterProcessMutex}

class OffsetCommitter(group: String, zk: CuratorFramework) {

  def start() = {
    if (zk.getState != CuratorFrameworkState.STARTED) {
      zk.start()
      zk.blockUntilConnected()
    }
  }

  def close() = zk.close()

  def getOffsets(topicPartitions: Iterable[TopicPartition]): OffsetMap = {
     topicPartitions.flatMap { topicPartition =>
       val (topic, partition) = topicPartition
       val path = getPartitionPath(group, topic, partition)
       Option(zk.checkExists.forPath(path)) match {
         case None => List()
         case Some(filestats) =>
           val bytes = zk.getData.forPath(path)
           val str = new String(bytes, Charsets.UTF_8).trim
           val offset = java.lang.Long.parseLong(str)
           List(topicPartition -> offset)
       }
     }.toMap
  }

  def setOffsets(offsets: OffsetMap): OffsetMap = {
    offsets foreach { case (topicPartition, offset) =>
      val (topic,partition) = topicPartition
      val nodePath = getPartitionPath(group, topic, partition)
      val bytes = offset.toString.getBytes(Charsets.UTF_8)
      Option(zk.checkExists.forPath(nodePath)) match {
        case None =>
          zk.create.creatingParentsIfNeeded.forPath(nodePath, bytes)
        case Some(fileStats) =>
          zk.setData().forPath(nodePath, bytes)
      }
    }
    getOffsets(offsets.keys)
  }

  def getLock: InterProcessLock = {
    val lockPath = s"/locks/kafka-rx/$group"
    new InterProcessMutex(zk, lockPath)
  }
  
  def getPartitionLock(topicPartition:TopicPartition) : InterProcessLock = {
    val (topic,partition) = topicPartition
    val lockPath = s"/locks/kafka-rx/$topic.$group.$partition"
    new InterProcessMutex(zk, lockPath)
  }
  
  def withPartitionLocks[T](partitions: Iterable[TopicPartition])(callback: => T): T = {
    val locks = partitions.map(getPartitionLock)
    try {
      locks.foreach(_.acquire)
      callback
    } catch {
      case err: Throwable =>
        err.printStackTrace()
        throw err
    } finally {
      locks.foreach(_.release)
    }
  }

  def commit(offsets: OffsetMap, userMerge: OffsetMerge, managerMerge: OffsetMerge): OffsetMap = {
    withPartitionLocks(offsets.keys) {
      val zkOffsets = getOffsets(offsets.keys)
      val userReconciledOffsets: OffsetMap = userMerge(zkOffsets, offsets)
      setOffsets(managerMerge(zkOffsets, userReconciledOffsets))
    }
  }

}