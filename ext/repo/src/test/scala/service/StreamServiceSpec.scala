package fauna.repo.service.test

import fauna.atoms._
import fauna.exec._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.repo._
import fauna.repo.cassandra.CassandraService
import fauna.repo.doc._
import fauna.repo.service.stream._
import fauna.repo.store._
import fauna.repo.test.{ IndexConfig, _ }
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.ops._
import scala.annotation.tailrec
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class StreamServiceSpec extends Spec {
  val ctx = CassandraHelper.context("repo")

  def TS(ts: Long) = Timestamp.ofMicros(ts)
  def newScope = ScopeID(ctx.nextID())

  // drop dispatchTS because it is not deterministic
  def dropDispatchTS(txns: Seq[TxnResult]): Seq[(Timestamp, Vector[Write])] =
    txns.map { t => (t.txnTS, t.writes) }

  "StreamService" - {
    val service = CassandraService.instance

    "can stream a single document" in {
      val scope = newScope
      val docID = DocID(SubID(1), CollectionID(1))
      val expected = List.newBuilder[TxnResult]

      def performWrites() = {
        // A version matching the expected DocID
        val data = MapV("foo" -> "bar").toData
        val res1 = ctx !! Store.insertUnmigrated(scope, docID, data)

        expected += TxnResult(
          res1.transactionTS,
          Clock.time,
          Vector(
            VersionAdd(
              scope,
              docID,
              Unresolved,
              Create,
              SchemaVersion.Min,
              data,
              None)))

        // A version from a different collection
        ctx ! Store.insertUnmigrated(
          scope,
          DocID(SubID(1), CollectionID(2)),
          Data.empty)

        // A version with a different SubID
        ctx ! Store.insertUnmigrated(
          scope,
          DocID(SubID(2), CollectionID(1)),
          Data.empty)

        val res2 = ctx !! Seq(
          Store.insertUnmigrated(scope, docID, data),
          Store.insertUnmigrated(
            scope,
            DocID(SubID(1), CollectionID(2)),
            Data.empty),
          Store.insertUnmigrated(scope, docID, data),
          Store.insertUnmigrated(scope, DocID(SubID(2), CollectionID(1)), Data.empty)
        ).join

        // Multiple writes to the same key will be compressed
        val diff = MapV("ts" -> res1.transactionTS.micros).toDiff
        expected += TxnResult(
          res2.transactionTS,
          Clock.time,
          Vector(
            VersionAdd(
              scope,
              docID,
              Unresolved,
              Update,
              SchemaVersion.Min,
              data,
              Some(diff))))
      }

      val stream =
        service.streamService.forDocument(scope, docID) flatMap { res =>
          if (res.writes.isEmpty) { // stream start
            performWrites()
            Observable.empty
          } else {
            Observable.single(res)
          }
        }

      dropDispatchTS(await(stream.takeF(2))) should
        contain.theSameElementsInOrderAs(dropDispatchTS(expected.result()))
    }

    "can stream an index term" in {
      val scope = newScope
      val idxID = IndexID(1)
      val collID = CollectionID(1)
      val docID = DocID(SubID(1), collID)
      val docID2 = DocID(SubID(2), collID)
      val idxCfg = IndexConfig(scope, idxID, collID)
      val terms = Vector(IndexTerm(StringV("foo")))
      val expected = List.newBuilder[TxnResult]

      def performWrites() = {
        // Expected term to match
        val res1 = ctx !! IndexStore.add(
          IndexRow(
            IndexKey(scope, idxID, Vector(IndexTerm(StringV("foo")))),
            IndexValue(scope, docID),
            Indexer.Empty),
          None)

        expected += TxnResult(
          res1.transactionTS,
          Clock.time,
          Vector(
            SetAdd(
              scope,
              idxID,
              Vector(StringV("foo")),
              Vector.empty,
              docID,
              Unresolved,
              Add)))

        // Different term
        ctx ! IndexStore.add(
          IndexRow(
            IndexKey(scope, idxID, Vector(IndexTerm(StringV("bar")))),
            IndexValue(scope, docID),
            Indexer.Empty),
          None)

        val res2 = ctx !! Seq(
          IndexStore.add(
            IndexRow(
              IndexKey(scope, idxID, Vector(IndexTerm(StringV("bar")))),
              IndexValue(scope, docID),
              Indexer.Empty),
            None),
          IndexStore.add(
            IndexRow(
              IndexKey(scope, idxID, Vector(IndexTerm(StringV("foo")))),
              IndexValue(scope, docID),
              Indexer.Empty),
            None),
          IndexStore.add(
            IndexRow(
              IndexKey(scope, IndexID(2), Vector(IndexTerm(StringV("foo")))),
              IndexValue(scope, docID),
              Indexer.Empty),
            None),
          IndexStore.add(
            IndexRow(
              IndexKey(scope, idxID, Vector(IndexTerm(StringV("foo")))),
              IndexValue(scope, docID2),
              Indexer.Empty),
            None)
        ).join

        expected += TxnResult(
          res2.transactionTS,
          Clock.time,
          Vector(
            SetAdd(
              scope,
              idxID,
              Vector(StringV("foo")),
              Vector.empty,
              docID,
              Unresolved,
              Add),
            SetAdd(
              scope,
              idxID,
              Vector(StringV("foo")),
              Vector.empty,
              docID2,
              Unresolved,
              Add)
          )
        )
      }

      val stream =
        service.streamService.forIndex(idxCfg, terms) flatMap { res =>
          if (res.writes.isEmpty) { // start event
            performWrites()
            Observable.empty
          } else {
            Observable.single(res)
          }
        }

      dropDispatchTS(await(stream.takeF(2))) should
        contain.theSameElementsInOrderAs(dropDispatchTS(expected.result()))
    }

    "can stream schema changes" in {
      val scope = newScope
      val docID = DatabaseID(256).toDocID
      val stream = service.streamService.forCollSet(None, Set(CollectionID(0)))
      val events = stream.takeF(3)

      val expected = List.newBuilder[TxnResult]

      // A version matching the expected DocID
      val data = MapV("foo" -> "bar").toData
      val res1 = ctx !! Store.insertUnmigrated(scope, docID, data)

      expected += TxnResult(
        res1.transactionTS,
        Clock.time,
        Vector(
          VersionAdd(
            scope,
            docID,
            Unresolved,
            Create,
            SchemaVersion.Min,
            data,
            None)))

      // Versions from different schemas
      ctx ! Seq(
        Store.insertUnmigrated(scope, CollectionID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope, IndexID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope, TokenID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope, UserFunctionID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope, RoleID(1).toDocID, Data.empty)
      ).join

      // A update on the schema
      val data2 = MapV("foo" -> "baz").toData
      val res2 = ctx !! Store.insertUnmigrated(scope, docID, data2)

      val diff = MapV("ts" -> res1.transactionTS.micros, "foo" -> "bar").toDiff
      expected += TxnResult(
        res2.transactionTS,
        Clock.time,
        Vector(
          VersionAdd(
            scope,
            docID,
            Unresolved,
            Update,
            SchemaVersion.Min,
            data2,
            Some(diff))))

      // Remove the schema
      val res3 = ctx !! Store.removeVersionUnmigrated(
        scope,
        docID,
        VersionID(res2.transactionTS, Create))
      expected += TxnResult(
        res3.transactionTS,
        Clock.time,
        Vector(
          VersionRemove(
            scope,
            docID,
            Resolved(res2.transactionTS, res2.transactionTS),
            Update)))

      dropDispatchTS(await(events)) should contain.theSameElementsInOrderAs(
        dropDispatchTS(expected.result()))
    }

    "can stream schema changes in a specific scope" in {
      val scope1 = newScope
      val scope2 = newScope
      val dbID = DatabaseID(256).toDocID
      val collID = CollectionID(256).toDocID
      val keyID = KeyID(256).toDocID
      val stream = service.streamService.forCollSet(
        Some(scope1),
        Set(DatabaseID.collID, CollectionID.collID, KeyID.collID))
      val events = stream.takeF(4)

      val expected = List.newBuilder[TxnResult]

      // A version matching the expected DocID
      val data = MapV("foo" -> "bar").toData
      val res1 = ctx !! Store.insertUnmigrated(scope1, dbID, data)

      // should be ignored
      ctx ! Store.insertUnmigrated(scope2, dbID, data)

      expected += TxnResult(
        res1.transactionTS,
        Clock.time,
        Vector(
          VersionAdd(
            scope1,
            dbID,
            Unresolved,
            Create,
            SchemaVersion.Min,
            data,
            None)))

      // Versions from different schemas
      val res2 = ctx !! Seq(
        Store.insertUnmigrated(scope1, collID, Data.empty),
        Store.insertUnmigrated(scope1, keyID, Data.empty),
        Store.insertUnmigrated(scope1, IndexID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope1, TokenID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope1, UserFunctionID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope1, RoleID(1).toDocID, Data.empty),

        // should be ignored
        Store.insertUnmigrated(scope2, collID, Data.empty),
        Store.insertUnmigrated(scope2, keyID, Data.empty),
        Store.insertUnmigrated(scope2, IndexID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope2, TokenID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope2, UserFunctionID(1).toDocID, Data.empty),
        Store.insertUnmigrated(scope2, RoleID(1).toDocID, Data.empty)
      ).join

      expected += TxnResult(
        res2.transactionTS,
        Clock.time,
        Vector(
          VersionAdd(
            scope1,
            collID,
            Unresolved,
            Create,
            SchemaVersion.Min,
            Data.empty,
            None),
          VersionAdd(
            scope1,
            keyID,
            Unresolved,
            Create,
            SchemaVersion.Min,
            Data.empty,
            None)
        )
      )

      // A update on the schema
      val data2 = MapV("foo" -> "baz").toData
      val res3 = ctx !! Store.insertUnmigrated(scope1, dbID, data2)

      // should be ignored
      ctx ! Store.insertUnmigrated(scope2, dbID, data2)

      val diff = MapV("ts" -> res1.transactionTS.micros, "foo" -> "bar").toDiff
      expected += TxnResult(
        res3.transactionTS,
        Clock.time,
        Vector(
          VersionAdd(
            scope1,
            dbID,
            Unresolved,
            Update,
            SchemaVersion.Min,
            data2,
            Some(diff))))

      // Remove the schema
      val res4 = ctx !! Store.removeVersionUnmigrated(
        scope1,
        dbID,
        VersionID(res3.transactionTS, Create))
      expected += TxnResult(
        res4.transactionTS,
        Clock.time,
        Vector(
          VersionRemove(
            scope1,
            dbID,
            Resolved(res3.transactionTS, res3.transactionTS),
            Update)))

      // should be ignored
      ctx !! Store.removeVersionUnmigrated(
        scope2,
        dbID,
        VersionID(res3.transactionTS, Create))

      dropDispatchTS(await(events)) should contain.theSameElementsInOrderAs(
        dropDispatchTS(expected.result()))
    }

    "can subscribe/unsubscribe concurrently" in {
      val scope = newScope
      val docID = DocID(SubID(1), CollectionID(1))
      val subscribed = Promise[Unit]()

      @tailrec
      def concurrentlyGetStream(times: Int = 100): Future[Option[TxnResult]] = {
        val stream = service.streamService.forDocument(scope, docID)
        if (times == 0) {
          stream
            .map { res =>
              subscribed.setDone()
              res
            }
            .select { _.writes.nonEmpty }
            .firstF
        } else {
          val subscription = stream.subscribe(new Observer.Default[TxnResult] {
            override def onError(cause: Throwable): Unit = () // ignore
          })
          // Simulate a client releasing the stream while a new one requests it.
          Future { subscription.cancel() }
          concurrentlyGetStream(times - 1)
        }
      }

      val resultF = concurrentlyGetStream()
      await(subscribed.future)

      val data = MapV("foo" -> "bar").toData
      val res = ctx !! Store.insertUnmigrated(scope, docID, data)

      val txnResult = await(resultF).value
      txnResult.txnTS shouldBe res.transactionTS
      txnResult.writes shouldBe Vector(
        VersionAdd(
          scope,
          docID,
          Unresolved,
          Create,
          SchemaVersion.Min,
          data,
          None
        ))
    }

    "failed subscriber does not block transaction pipeline" - {

      def test(fn: (ScopeID, DocID) => Observable[TxnResult]) = {
        val scope = newScope
        val docID = DocID(SubID(1), CollectionID(1))

        val resF =
          fn(scope, docID) foreachF { _ =>
            throw new IllegalStateException()
          }

        ctx !! Store.insertUnmigrated(scope, docID, Data.empty)
        an[IllegalStateException] shouldBe thrownBy { await(resF) }
        noException shouldBe thrownBy {
          ctx !! Store.insertUnmigrated(scope, docID, Data.empty)
        }
      }

      "for data node streams" in test { (scope, docID) =>
        service.streamService.forDocument(scope, docID)
      }

      "for log node streams" in test { (scope, docID) =>
        service.streamService.forCollSet(Some(scope), Set(docID.collID))
      }
    }

    "get idle events" in {
      val scope = newScope
      val docID = DocID(SubID(1), CollectionID(1))

      val resultsF =
        service.streamService
          .forDocument(scope, docID, idlePeriod = Duration.Zero) // get all idles
          .takeF(2) // start event + 1 idle event

      val writes = await(resultsF) map { _.writes }
      all(writes) shouldBe empty
    }
  }
}
