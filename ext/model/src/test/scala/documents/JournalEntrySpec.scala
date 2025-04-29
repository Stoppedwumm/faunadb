package fauna.model.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.model._
import fauna.repo.test.CassandraHelper
import fauna.repo.RepoContext.Result
import fauna.storage.doc.{ Data, Field }
import scala.concurrent.duration._

class JournalEntrySpec extends Spec {
  val ctx = CassandraHelper.context("model")

  def TS(ts: Long) = Timestamp.ofMicros(ts)

  val NameField = Field[String]("name")

  "JournalEntrySpec" - {
    "manipulates entries" in {
      val data = Data(NameField -> "foo")
      val localhost = HostID.randomID

      val Result(txnTS, entry) = ctx !! JournalEntry.write(localhost, "test", data)

      val entries1 = ctx ! JournalEntry.latestByHostAndTag(localhost, "test")
      entries1.isEmpty should be (false)
      entries1 foreach { e =>
        e.id should equal (entry.get.id)
        e.host should equal (localhost)
        e.tag should equal ("test")
        val ttlDuration = e.data(JournalEntry.TTLField).get.difference(txnTS)
        // Will be slightly off: the transaction time is slightly after the snapshot time.
        (ttlDuration >= JournalEntry.TTL - 1.minute) should be(true)
        (ttlDuration <= JournalEntry.TTL + 1.minute) should be(true)
        e.data(NameField) should equal (data(NameField))
      }

      entries1 foreach { entry =>
        entry.host should equal (localhost)
        entry.tag should equal ("test")
        ctx ! JournalEntry.remove(entry.id)
      }

      val entries2 = ctx ! JournalEntry.latestByHostAndTag(localhost, "test")
      entries2 should equal (None)
    }

    "latestByHostAndTag reads latest entry" in {
      val localhost = HostID.randomID

      val first = ctx ! JournalEntry.write(localhost, "tag", Data.empty)

      val before = ctx ! JournalEntry.latestByHostAndTag(localhost, "tag")
      before.isEmpty should be (false)
      before foreach { e =>
        e.id should equal (first.get.id)
      }

      val second = ctx ! JournalEntry.write(localhost, "tag", Data.empty)

      val after = ctx ! JournalEntry.latestByHostAndTag(localhost, "tag")
      after.isEmpty should be (false)
      after foreach { e =>
        e.id should equal (second.get.id)
      }

    }
  }
}
