package com.cj.kafka.rx

import org.scalatest._

class CommittableTest extends FlatSpec with Matchers {

  "Committable" should "pass along a commit context to derivatives" in {
    var passing = false

    val committable = new Committable[String] {
      override def value: String = "cats"
      override def commit(offsetMerge: OffsetMerge): OffsetMap = {
        passing = true
        Map()
      }
    }

    val life = committable.derive(42)
    life.commit()

    life.value should be(42)
    passing should be(true)
  }

  it should "map the value of a comittable given a function" in {
    //given
    val committable = new Committable[Int] {
      override def value: Int = 1

      override def commit(offsetMerge: OffsetMerge): OffsetMap = ???
    }


    def fn(x: Int) = {x + 1}

    //when
    val newCommittable = committable.map(fn)

    //then
    newCommittable.value should be (fn(committable.value))


  }

  it should "return a commitable with the same commit function after mapping" in {
    //given
    var isCommitted = false
    val committable = new Committable[Int] {
      override def value: Int = 1

      override def commit(offsetMerge: OffsetMerge): OffsetMap = {
        isCommitted = true
        Map()
      }
    }

    def fn(x: Int) = {x + 1}

    //when
    val newCommittable = committable.map(fn)
    newCommittable.commit()

    //then
    isCommitted should be (true)

  }

}
