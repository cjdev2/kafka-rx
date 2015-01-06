package com.cj.kafka.rx

import com.google.common.base.Charsets
import com.google.common.primitives.Longs
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.test.TestingServer
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.curator.utils.EnsurePath

import scala.concurrent.duration._

import org.scalatest.{BeforeAndAfter, FlatSpec, ShouldMatchers}

class OffsetCommitterIntegrationTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

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
    val zk = new OffsetCommitter("topic", "group", client)
    zk.getOffsets should be(Map[Int, Long]())
  }

  it should "get offsets from zookeeper" in {
    val zk = new OffsetCommitter("test", "test", client)
    val partition = 42
    val path = KafkaHelper.getPartitionPath(zk.offsetPath, partition)
    val expectedOffset = 1337L
    val bytes = expectedOffset.toString.getBytes
    val expectedOffsets = Map[Int, Long](partition -> expectedOffset)

    new EnsurePath(path).ensure(client.getZookeeperClient)
    client.setData().forPath(path, bytes)

    zk.getOffsets should be(expectedOffsets)
  }

  it should "write new offsets to zookeeper" in {
    val zk = new OffsetCommitter("topic", "group", client)
    val offsets = Map[Int, Long](1 -> 2)

    zk.setOffsets(offsets)
    zk.getOffsets should be(offsets)
  }

  it should "encode offsets as strings" in {
    val zk = new OffsetCommitter("topic", "group", client)
    val partition = 1
    val path = KafkaHelper.getPartitionPath(zk.offsetPath, partition)

    val offsets = Map[Int, Long](partition -> 42)

    zk.setOffsets(offsets)

    val bytes = client.getData.forPath(path)
    val str = new String(bytes, Charsets.UTF_8)
    str should be(offsets(partition).toString)
  }

  it should "update existing offsets in zookeeper" in {
    val zk = new OffsetCommitter("topic", "group", client)
    val offsets = Map[Int, Long](1 -> 0, 2 -> 0)
    val otherOffsets = Map[Int, Long](1 -> 1)

    zk.setOffsets(offsets)
    zk.getOffsets should be(offsets)

    val zkOffsets = zk.setOffsets(otherOffsets)
    zkOffsets should be(Map(1 -> 1L, 2 -> 0L))
  }

  it should "provide locks" in {
    val zk = new OffsetCommitter("topic", "group", client)
    val lock = zk.getLock
    if (lock.acquire(100, SECONDS)) {
      try {
        "the lock" should include("lock")
      } finally {
        lock.release()
      }
    } else {
      "failed to acquire lock" should be("impossible")
    }
  }

  it should "provide a commit hook for doing work within a zk lock" in {
    val zk = new OffsetCommitter("topic", "group", client)
    val mgr = new OffsetManager[Array[Byte]]
    val offsets = Map[Int, Long](1 -> 0, 2 -> 0)
    val otherOffsets = Map[Int, Long](1 -> 1, 2 -> 1)

    zk.setOffsets(offsets)

    zk.commit(mgr, otherOffsets, { zkOffsets =>
      zkOffsets should be(offsets)
    })
  }
}