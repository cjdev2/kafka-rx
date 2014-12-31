package com.cj.kafka.rx

import org.apache.curator.utils.ZKPaths

object ZKHelper {

  def extractPartition(path: String): Int = {
    val part = ZKPaths.getNodeFromPath(path)
    Integer.parseInt(part)
  }

  def getPartitionPath(base: String, part: Int) = ZKPaths.makePath(base, part.toString)

  def getConsumerOffsetPath(topic: String, group: String) = s"/consumers/$group/offsets/$topic"

}