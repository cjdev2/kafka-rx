package com.cj.kafka.rx

import org.scalatest._

class CommittableTest extends FlatSpec with ShouldMatchers {

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

}
