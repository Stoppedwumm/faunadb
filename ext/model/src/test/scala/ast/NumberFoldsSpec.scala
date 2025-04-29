package fauna.model.test

import fauna.ast._
import fauna.prop._
import fauna.prop.test.PropSpec
import fauna.repo.query.QDone
import org.scalatest.matchers.should.Matchers

class NumberFoldsSpec extends PropSpec(10, 100) with Matchers {
  // Anecdotally, on my machine the longest list a stack-recursive
  // implementation can handle is 1267 elements, so testing with 10000
  // should be plenty.
  val listLen = 10000

  prop("tail recurses for longs") {
    for {
      l <- Prop.long.times(listLen)
    } {
      val sum = l.foldLeft(0L) { (s, e) => s + e }
      val add = AddFunction.apply(l map { Right(_) } toList, null, null)

      add should equal (QDone(Right(LongL(sum))))
    }
  }

  prop("tail recurses for doubles") {
    for {
      l <- Prop.double.times(listLen)
    } {
      val sum = l.foldLeft(0d) { (s, e) => s + e }
      val add = AddFunction.apply(l map { Left(_) } toList, null, null)

      add should equal (QDone(Right(DoubleL(sum))))
    }
  }

  prop("tail recurses for mixture of doubles and longs") {
    for {
      l <- Prop.either(Prop.double, Prop.long).times(listLen)
    } {
      val sum = l.foldLeft(0d) { (s, e) => s + e.fold(x => x, x => x.toDouble) }

      AddFunction.apply(l.toList, null, null) foreach {
        case Right(DoubleL(v)) => v should equal (sum)
        case r => fail(s"unknown result $r")
      }
    }
  }

  once("reports errors for longs") {
    for {
      i <- Prop.long
      j <- Prop.long
      k <- Prop.const(0L)
    } {
      val l = Right(i) :: Right(j) :: Right(k) :: Nil
      DivideFunction.apply(l, null, RootPosition) should equal (QDone(Left(List(DivideByZero(RootPosition)))))
    }
  }

  once("reports errors for doubles") {
    for {
      i <- Prop.double
      j <- Prop.double
      k <- Prop.const(0D)
    } {
      val l = Left(i) :: Left(j) :: Left(k) :: Nil
      DivideFunction.apply(l, null, RootPosition) should equal (QDone(Left(List(DivideByZero(RootPosition)))))
    }
  }
}
