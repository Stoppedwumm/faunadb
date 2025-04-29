package fauna.repo.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.exec.FaunaExecutionContext
import fauna.lang._
import fauna.repo.doc.Version
import fauna.repo.query._
import fauna.repo.schema.migration.MigrationList
import fauna.repo.values.Value
import fauna.storage._
import fauna.storage.api.version.{ DocSnapshot, StorageVersion }
import fauna.storage.doc._
import fauna.storage.ir._
import fauna.storage.ops.VersionAdd
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class ReadCacheSpec extends Spec {
  import ReadCache._

  private var cache: ReadCache = _

  before {
    cache = new ReadCache()
  }

  private val scopeID = ScopeID.RootID
  private val docID = DocID.MinValue
  private val validTS = Timestamp.MaxMicros
  private val read = DocSnapshot(scopeID, docID, validTS)
  private val noResult = DocSnapshot.Result(version = None)
  private val version =
    StorageVersion.newByteBuf(
      scopeID,
      docID,
      Unresolved,
      Create,
      SchemaVersion.Min,
      None,
      CBOR.encode(
        MapV(
          "data" -> MapV(
            "fizz" -> "buzz",
            "foo" -> MapV(
              "bar" -> "baz"
            )
          )
        )),
      CBOR.encode(NullV)
    )
  private val result =
    DocSnapshot.Result(version = Some(version))
  private val write =
    VersionAdd(
      scopeID,
      docID,
      Unresolved,
      Create,
      SchemaVersion.Min,
      Data.empty,
      None
    )
  private val noPrefix: Prefix = Prefix.empty
  private val prefix: Prefix = Prefix.fromSpecific(Seq(write))

  private def getOrLoad(prefix: Prefix, result: DocSnapshot.Result) = {
    var miss = false
    val resF =
      cache.getOrLoad(prefix, read) {
        miss = true
        Future.successful(result)
      }
    (await(resF), miss)
  }

  val dummySrcHint = ReadCache.CachedDoc.SetSrcHint(
    scopeID,
    IndexID.MaxValue,
    "dummy",
    CollectionID.MaxValue,
    "dummy",
    Seq.empty,
    fql.ast.Span.Null
  )

  private def put(prefix: Prefix, partials: Partials) =
    cache.put(prefix, scopeID, docID, validTS, partials, srcHint = dummySrcHint)

  private def peek(prefix: Prefix) =
    cache.peek(prefix, scopeID, docID, validTS)

  "ReadCache" - {
    "getOrLoad" - {
      "misses first load" in {
        val (res, miss) = getOrLoad(noPrefix, result)
        res shouldBe result
        assert(miss)
      }

      "returnes cached value, if present" in {
        getOrLoad(noPrefix, result) // cache load
        val (res, miss) = getOrLoad(noPrefix, noResult)
        res shouldBe result
        assert(!miss)
      }

      "refreshes on different prefix" in {
        getOrLoad(noPrefix, noResult) // cache load
        val (res, miss) = getOrLoad(prefix, result) // refresh
        res shouldBe result
        assert(miss)
      }
    }

    "put/peek" - {
      "peek knows knowting at first" in {
        peek(noPrefix) shouldBe empty
      }

      "peek sees partials" in {
        put(noPrefix, Map(("data" :: "fizz" :: Nil) -> Value.Str("buzz")))
        put(noPrefix, Map(("data" :: "foo" :: "bar" :: Nil) -> Value.Str("baz")))

        inside(peek(noPrefix).value) { case p: CachedDoc.Partial =>
          p.project("non-existing" :: Nil) shouldBe empty
          p.project("data" :: Nil).value should matchPattern {
            case _: Fragment.Struct =>
          }
          p.project("data" :: "foo" :: Nil).value should matchPattern {
            case _: Fragment.Struct =>
          }
          inside(p.project("data" :: "fizz" :: Nil).value) {
            case f: Fragment.Value => f.unwrap shouldBe Value.Str("buzz")
          }
          inside(p.project("data" :: "foo" :: "bar" :: Nil).value) {
            case f: Fragment.Value => f.unwrap shouldBe Value.Str("baz")
          }
        }
      }

      "peek sees read documents" in {
        val partials = Map(("data" :: "fizz" :: Nil) -> Value.Str("buzz"))
        put(noPrefix, partials) // put partians
        getOrLoad(noPrefix, result) // override partials

        inside(peek(noPrefix).value) { case v: CachedDoc.Version =>
          v.decode(MigrationList.empty) shouldBe Version.decode(version)
        }
      }

      "put do not override known value" in {
        val data =
          Value.Struct(
            "fizz" -> Value.Str("buzz"),
            "foo" -> Value.Struct(
              "bar" -> Value.Str("baz")
            )
          )

        put(noPrefix, Map(("data" :: Nil) -> data))
        put(noPrefix, Map(("data" :: "foo" :: "bar" :: Nil) -> Value.Str("baz")))

        inside(peek(noPrefix).value) { case p: CachedDoc.Partial =>
          inside(p.project("data" :: Nil).value) { case v: Fragment.Value =>
            v.unwrap shouldBe data
          }
        }
      }

      "put do not override read version" in {
        getOrLoad(noPrefix, result) // cache load
        put(noPrefix, Map(("data" :: "fizz" :: Nil) -> Value.Str("buzz")))
        peek(noPrefix).value should matchPattern { case _: CachedDoc.Version => }
      }

      "peek sees deleted versions after read" in {
        getOrLoad(prefix, noResult) // load an empty snapshot
        peek(prefix).value should matchPattern { case _: CachedDoc.DocNotFound => }
      }

      "peek tracks write prefix" in {
        put(noPrefix, Map(("data" :: "fizz" :: Nil) -> Value.Str("buzz")))
        peek(prefix) shouldBe empty // knows nothing on the different prefix
      }
    }

    "inspect" - {
      "can iterate concurrently" in {
        import FaunaExecutionContext.Implicits.global

        val futs = List.newBuilder[Future[_]]
        var prefix = noPrefix

        for (_ <- 1 to 100) {
          futs += Future {
            prefix = prefix :+ write // ensure different prefixes
            getOrLoad(prefix, result)
          }
          futs += Future {
            cache.inspect {
              _.iterator foreach { _ => () }
            }
          }
        }

        noException should be thrownBy {
          val res = Future.sequence(futs.result())
          Await.result(res, 5.seconds)
        }
      }
    }
  }

  "ReadCache.CachedDoc" - {
    "pin/unpin" - {
      def cachedDoc = put(prefix, Map(("foo" :: Nil) -> Value.Str("bar")))

      "partials are pinned" in {
        assert(cachedDoc.isPinned)
      }

      "can unpin" in {
        val doc = cachedDoc
        doc.unpin()
        assert(!doc.isPinned)
        doc.unpin()
        assert(!doc.isPinned)
      }

      "can re-pin" in {
        val doc = cachedDoc
        doc.unpin()
        doc.unpin() // double unpin it so we know internal counts are correct.
        assert(!doc.isPinned)
        doc.pin()
        assert(doc.isPinned)
      }

      "is re-pinned on put" in {
        val doc = cachedDoc
        doc.unpin()
        assert(!doc.isPinned)
        cachedDoc // put again
        assert(doc.isPinned)
      }

      "can pin multiple times" in {
        val doc = cachedDoc // starts pinned
        doc.pin() // pins it again
        assert(doc.isPinned)
        doc.unpin()
        assert(doc.isPinned)
        doc.unpin()
        assert(!doc.isPinned)
      }

      "cache loads are not pinned" in {
        getOrLoad(prefix, result)
        assert(!peek(prefix).value.isPinned)
      }

      "cache loads preserves pins" in {
        val doc = cachedDoc
        assert(doc.isPinned)

        getOrLoad(prefix, result)
        inside(peek(prefix).value) { case v: CachedDoc.Version =>
          assert(v.isPinned)
        }
      }
    }
  }
}
