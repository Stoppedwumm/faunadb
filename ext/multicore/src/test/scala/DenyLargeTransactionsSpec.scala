package fauna.multicore.test

import fauna.atoms._
import fauna.codex.json._
import fauna.config.CoreConfig
import fauna.model.Index
import fauna.prop.api.{ CoreLauncher, DefaultQueryHelpers }
import fauna.storage.index.IndexKey
import fauna.storage.ops.Write
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration._

class DenyLargeTransactionsSpec extends Spec with DefaultQueryHelpers with Eventually {
  val admin1 = CoreLauncher.adminClient(1, mcAPIVers)

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)

  override protected def afterAll() =
    CoreLauncher.terminateAll()

  override protected def beforeAll(): Unit = {
    CoreLauncher.launch(1, APIVersion.Default.toString)

    init(admin1, "dc1")

    waitForPing(Seq(api1))

    val cls = CreateClass(MkObject("name" -> "foo"))
    api1.query(cls, rootKey) should respond (Created)

    eventually(timeout(scaled(5.seconds))) {
      val res = api1.query(Get(ClassRef("foo")), rootKey)
      res should respond (OK)
    }

    val idx = CreateIndex(MkObject(
      "name" -> "boom",
      "source" -> ClassRef("foo"),
      "terms"  -> JSArray(
        MkObject("field" -> JSArray("data", "bomb0"))),
      "values" -> JSArray(
        MkObject("field" -> JSArray("data", "bomb1")))))
    val x = api1.query(idx, rootKey)
    x should respond (Created)
    
    eventually(timeout(scaled(5.seconds))) {
      val res = api1.query(Get(IndexRef("boom")), rootKey)
      res should respond (OK)
    }
  }

  "Cluster should" - {
    "allow keys under threshold" in {
      bigTxnKey(IndexKey.KeyBytesThreshold - 43) should respond (OK)
    }

    "deny keys over threshold" in {
      bigTxnKey(IndexKey.KeyBytesThreshold - 42) should respond (BadRequest)
    }

    def bigTxnKey(size: Int) = {
      val data = MkObject(
        "bomb0" -> Bytes(new Array[Byte](Short.MaxValue - 13)),
        "bomb1" -> Bytes(new Array[Byte](size - Short.MaxValue)))
      val doc = Select("ts", CreateF(ClassRef("foo"), MkObject("data" -> data)))
      api1.query(doc, rootKey)
    }

    "allow docs under threshold" in {
      bigDoc(Write.VersionBytesThreshold - 1) should respond (OK)
    }

    "deny docs over threshold" in {
      bigDoc(Write.VersionBytesThreshold + 1) should respond (BadRequest)
    }

    def bigDoc(size: Int) = {
      val chunks = 3
      val data0 = MkObject("unindexed-0" -> Bytes(new Array[Byte](size/chunks)))
      val data1 = MkObject("unindexed-1" -> Bytes(new Array[Byte](size/chunks)))
      val data2 = MkObject("unindexed-2" -> Bytes(new Array[Byte](size/chunks - 56)))

      val doc0 = Select("ref", CreateF(ClassRef("foo"), MkObject("data" -> data0)))
      
      val res = api1.query(doc0, rootKey)
      res should respond (OK)
      val ref = res.json / "resource"
      
      val doc1 = Select("ts", Update(ref, MkObject("data" -> data1)))
      api1.query(doc1, rootKey) should respond (OK)
      
      val doc2 = Select("ts", Update(ref, MkObject("data" -> data2)))
      api1.query(doc2, rootKey)
    }

    "allow transactions under threshold" in {
      bigTxn((new CoreConfig().transaction_max_size_bytes * 0.95).toInt) should respond (OK)
    }

    "deny transactions over threshold" in {
      bigTxn((new CoreConfig().transaction_max_size_bytes * 1.05).toInt) should respond (BadRequest)
    }

    def bigTxn(size: Int) = {
      val noObjects = size / 500_000
      val chunkSize = size / noObjects
      val createObjs = (1 to noObjects) map { _ =>
        CreateF(
          ClassRef("foo"),
          MkObject("data" -> MkObject("unindexed" -> Var("bytes")))
        )
      }

      api1.query(Let("bytes" -> Bytes(new Array[Byte](chunkSize))) {
        Do((createObjs ++ Seq(JSTrue)): _*)
      }, rootKey)
    }
  }

  "synchronous index build with maximalized txn size succeeds" in {
    val colName = "sync-col"
    val colRef = ClassRef(colName)
    api1.query(CreateCollection(MkObject("name" -> colName)), rootKey) should respond(Created)

    // this is the maximum value at which we don't get into ComponentTooLarge territory
    val term = Bytes(new Array[Byte](Short.MaxValue - 11))

    (1 until Index.BuildSyncSize) foreach { _ =>
      api1.query(CreateF(colRef, MkObject("data" ->
        MkObject(
          "f1" -> term,
          // making this any larger would result in TransactionKeyTooLargeException
          "f2" -> Bytes(new Array[Byte](Short.MaxValue - 1579))
        )
      )), rootKey) should respond(Created)
    }

    val indexName = s"$colName-index-1"
    api1.query(
        CreateIndex(MkObject(
          "name" -> indexName,
          "source" -> colRef,
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "f1"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "f2"))),
        )),
      rootKey
    ) should respond(Created)

    eventually {
      // verify all docs were actually indexed (i.e. none were omitted due to e.g. too large components)
      api1.query(Get(IndexRef(indexName)), rootKey).resource / "active" should equal(JSTrue)
      val res = api1.query(
        Count(Paginate(Match(IndexRef(indexName), term), size = Index.BuildSyncSize)),
        rootKey
      )
      (res.resource / "data" / 0).as[Long] should equal(Index.BuildSyncSize - 1)
    }
  }
}
