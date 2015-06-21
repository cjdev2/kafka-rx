package com.cj.kafka.rx

import com.google.common.base.Charsets
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.test.TestingServer
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.curator.utils.EnsurePath

import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfter, FlatSpec, ShouldMatchers}

class ZookeeperOffsetCommitterIntegrationTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  var server: TestingServer = _
  var client: CuratorFramework = _

  before {
    server = new TestingServer()
    client = CuratorFrameworkFactory.newClient(server.getConnectString, new RetryUntilElapsed(500,50))
    server.start()
    client.start()
  }

  after {
    client.close()
    server.close()
    client = null
    server = null
  }

  "OffsetCommitter" should "return no offsets given no data" in {
    val zk = new ZookeeperOffsetCommitter("group", client)
    zk.getOffsets(List()) should be(Map[TopicPartition, Long]())
  }

  it should "get offsets from zookeeper for one topic and partition" in {
    val zk = new ZookeeperOffsetCommitter("test", client)
    val topic = "test"
    val partition = 42
    val path = getPartitionPath("test", "test", partition)
    val expectedOffset = 1337L
    val bytes = expectedOffset.toString.getBytes
    val expectedOffsets = Map[TopicPartition, Long](topic -> partition -> expectedOffset)

    new EnsurePath(path).ensure(client.getZookeeperClient)
    client.setData().forPath(path, bytes)

    zk.getOffsets(List(topic -> partition)) should be(expectedOffsets)
  }
  it should "get offsets from zookeeper for multiple topics" in {
    val zk = new ZookeeperOffsetCommitter("test", client)
    val topic1 = "topic1"
    val topic2 = "topic2"
    val partition = 42
    val path1 = getPartitionPath("test", topic1, partition)
    val path2 = getPartitionPath("test", topic2, partition)
    val expectedOffset = 1337L
    val bytes = expectedOffset.toString.getBytes
    val expectedOffsets1 = Map[TopicPartition, Long](topic1 -> partition -> expectedOffset)
    val expectedOffsets2 = Map[TopicPartition, Long](topic2 -> partition -> expectedOffset)

    new EnsurePath(path1).ensure(client.getZookeeperClient)
    client.setData().forPath(path1, bytes)

    new EnsurePath(path2).ensure(client.getZookeeperClient)
    client.setData().forPath(path2, bytes)

    zk.getOffsets(List(topic1 -> partition, topic2 -> partition)) should be(expectedOffsets1 ++ expectedOffsets2)
  }

  it should "write new offsets to zookeeper" in {
    val topic = "topic"
    val zk = new ZookeeperOffsetCommitter("group", client)
    val offsets = Map[TopicPartition, Long](topic -> 1 -> 2)

    zk.setOffsets(offsets)
    zk.getOffsets(offsets.keys) should be(offsets)
  }

  it should "encode offsets as strings" in {
    val topic = "topic"
    val zk = new ZookeeperOffsetCommitter( "group", client)
    val partition = 1
    val path = getPartitionPath("group", topic, partition)

    val offsets = Map[TopicPartition, Long](topic -> partition -> 42)

    zk.setOffsets(offsets)

    val bytes = client.getData.forPath(path)
    val str = new String(bytes, Charsets.UTF_8)
    str should be(offsets(topic -> partition).toString)
  }

  it should "update existing offsets in zookeeper for correct partition" in {
    val topic = "topic"
    val zk = new ZookeeperOffsetCommitter( "group", client)
    val existingOffsets = Map[TopicPartition, Long](topic -> 1 -> 0, topic -> 2 -> 0)
    val newOffsets = Map[TopicPartition, Long](topic -> 1 -> 1)

    //Set original ZK offsets
    zk.setOffsets(existingOffsets)
    zk.getOffsets(existingOffsets.keys) should be(existingOffsets)

    //Adding new offsets to ZK
    zk.setOffsets(newOffsets)
    val zkOffsets = zk.getOffsets(existingOffsets.keys)
    zkOffsets should be(Map(topic -> 1 -> 1L, topic -> 2 -> 0L))
  }

  it should "update existing offsets in zookeeper for correct topic" in {
    val topic1 = "topic1"
    val topic2 = "topic2"
    val zk = new ZookeeperOffsetCommitter( "group", client)
    val existingOffsets = Map[TopicPartition, Long](topic1 -> 1 -> 0, topic2 -> 1 -> 0)
    val newOffsets = Map[TopicPartition, Long](topic1 -> 1 -> 1)

    //Set original ZK offsets
    zk.setOffsets(existingOffsets)
    zk.getOffsets(existingOffsets.keys) should be(existingOffsets)

    //Adding new offsets to ZK
    zk.setOffsets(newOffsets)
    val zkOffsets = zk.getOffsets(existingOffsets.keys)
    zkOffsets should be(Map(topic1 -> 1 -> 1L, topic2 -> 1 -> 0L))
  }

  it should "provide locks" in {
    val zk = new ZookeeperOffsetCommitter("group", client)
    val lock = zk.getPartitionLock("test" -> 123)
    try {
      lock.acquire
      "the lock" should include("lock")
    } finally {
      lock.release
    }
  }

  it should "provide a commit hook for doing work within a zk lock" in {
    val topic = "topic"
    val zk = new ZookeeperOffsetCommitter("group", client)
    val offsets = Map[TopicPartition, Long](topic -> 1 -> 0, topic -> 2 -> 0)
    val otherOffsets = Map[TopicPartition, Long](topic -> 1 -> 1, topic -> 2 -> 1)

    zk.setOffsets(offsets)

    zk.commit(otherOffsets, { (zkOffsets, messageOffsets) =>
      zkOffsets should be(offsets)
      offsets
    })
  }

}