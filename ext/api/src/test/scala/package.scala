package fauna.api

import fauna.net._
import fauna.prop.test.{ PropSpec => PS }
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

package object test extends NetworkHelpers with HttpClientHelpers with Eventually {
  // This is 1 second higher than what is configured for tests.
  val SchemaCacheTTL = 3000

  def beforeTTLCacheExpiration(f: => Unit) =
    eventually(timeout((SchemaCacheTTL / 2).seconds), interval(200.millis)) {
      f
    }

}

package test {
  trait Spec extends AnyFreeSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers
  abstract class PropSpec extends PS(5, 200) with Matchers
}
