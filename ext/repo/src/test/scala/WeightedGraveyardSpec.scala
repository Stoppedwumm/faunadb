package fauna.repo.test

import fauna.atoms.HostID
import fauna.lang.clocks._
import fauna.repo.WeightedGraveyard
import scala.concurrent.duration._

class WeightedGraveyardSpec extends Spec {
  "WeightedGraveyard" - {
    "requires at least one live host" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        new WeightedGraveyard(Vector.empty, Vector.empty, 1.day)
      }
    }

    "yields a live hosts when the graveyard is empty" in {
      val a = HostID.randomID
      val b = HostID.randomID

      val g = new WeightedGraveyard(Vector(a, b), Vector.empty, 1.day)

      g.length should equal(2)
      g(0) should equal((a, 1.0))
      g(1) should equal((b, 1.0))
    }

    "yields graveyard hosts first" in {
      val dead = HostID.randomID
      val a = HostID.randomID
      val b = HostID.randomID

      val g = new WeightedGraveyard(Vector(a, b), Vector((dead, Clock.time)), 1.day)

      g.length should equal(3)
      g(0) should equal((dead, 1.0))
      g(1) should equal((a, 1.0))
      g(2) should equal((b, 1.0))
    }

    "TTLs graveyard hosts" in {
      val clock = new TestClock

      val dead = HostID.randomID
      val departure = clock.time
      val a = HostID.randomID

      val before =
        new WeightedGraveyard(Vector(a), Vector((dead, departure)), 1.day)(clock)

      before.length should equal(2)
      before(0) should equal((dead, 1.0))
      before(1) should equal((a, 1.0))

      clock.advance(1.day)

      val after =
        new WeightedGraveyard(Vector(a), Vector((dead, departure)), 1.day)(clock)

      after.length should equal(1)
      after(0) should equal((a, 1.0))
    }

    "weights graveyard hosts" in {
      val clock = new TestClock

      val long = HostID.randomID
      val short = HostID.randomID
      val a = HostID.randomID

      val epoch = clock.time

      clock.advance(7.days)

      val g = new WeightedGraveyard(
        Vector(a),
        Vector((short, epoch + 7.days), (long, epoch)),
        14.days)(clock)

      g.length should equal(3)
      g(0) should equal((long, 0.5))
      g(1) should equal((short, 1.0))
      g(2) should equal((a, 1.0))
    }
  }
}
