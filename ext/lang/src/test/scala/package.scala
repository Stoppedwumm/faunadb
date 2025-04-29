package fauna.lang

import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

package test {
  trait Spec extends AnyFreeSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers
}
