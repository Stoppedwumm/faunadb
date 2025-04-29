package fauna.snowflake.test

import fauna.lang.clocks.TestClock
import fauna.lang.Timestamp
import fauna.snowflake.IDSource
import scala.concurrent.duration._
import scala.util.Random

class SnowFlakeSpec extends Spec {

  // This value needs to match IDSource.SequenceBits, but
  // that value is private.
   val SequenceBits = 1024

  "SnowFlake" - {
    "works" in {
      // Simplest test case, get 2 stamps, with the default clock
      val idSource = new IDSource(() => 0)

      val id1 = idSource.getID
      val id2 = idSource.getID

      id1 should be < id2
    }

    "Stop Clock generate IDs" in {
      // Simplest test case, get 2 ids with the clock frozen in time
      val tclock = new TestClock(Timestamp.ofMillis(System.currentTimeMillis()))
      val idsource = new IDSource(() => 0, tclock)


      val id1 = idsource.getID
      val id2 = idsource.getID

      id1 should be < id2
    }

    "Generate max IDS with clock stopped" in {
      // Ensures we can get upto SequenceBits IDs with the clock frozen
      val tclock = new TestClock(Timestamp.ofMillis(System.currentTimeMillis()))
      val idsource = new IDSource(() => 0, tclock)

      var id =  idsource.getID

      // Already pulled 1 id, now pull all the rest
      for (_ <- 1 until SequenceBits) {
        val id2 = idsource.getID
        id should be < id2
        id = id2
      }
    }

    "Generated max ID with stop-n-go clock" in {
      // Stop the clock, get 1023 events, move the clock the smallest
      //   amount 1milli then try for another 1023 events
      //   ensuring each value is greater than the previous
      val tclock = new TestClock(Timestamp.ofMillis(System.currentTimeMillis()))
      val idsource = new IDSource(() => 0, tclock)

      var id = idsource.getID
      // Already pulled 1 id, now pull all the rest
      // of the IDs for this millisecond
      for (_ <- 1 until SequenceBits) {
        val id2 = idsource.getID
        id should be < id2
        id = id2
      }

      // Move the clock forward 1 milli second
      tclock.advance(1.milli)

      // Consume all ids, have not used any in this millisecond
      for (_ <- 0 until SequenceBits) {
        val id2 = idsource.getID
        id should be < id2
        id = id2
      }
    }

    "Multiple workers with stop-n-go clock" in {
      // Stop the clock, make 1000 workers
      //   check that no two works produce the same id
      //   advance the clock
      //   check that no two works produce the same id
      val NumWorkers = 1000
      val tclock = new TestClock(Timestamp.ofMillis(System.currentTimeMillis()))
      val workers = (0 until NumWorkers) map { w => new IDSource(() => w, tclock) }

      var results = Set.empty[Long]

      results += workers(0).getID
      // Already pulled 1 id, now pull all the rest for this millisecond
      for (_ <- 1 until SequenceBits) {
         val id2  = workers(Random.nextInt(NumWorkers)).getID
         results should not contain id2
         results += id2
      }

      // Move the clock forward 1 milli second
      tclock.advance(1.milli)

      // Consume all ids, have not used any in this millisecond
      for (_ <- 0 until SequenceBits) {
        val id2  = workers(Random.nextInt(NumWorkers)).getID
        results should not contain id2
        results += id2
      }
    }
  }
}
