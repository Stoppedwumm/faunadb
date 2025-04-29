package fauna.repo

import fauna.lang.{ ConsoleControl, Timestamp }
import fauna.lang.clocks.Clock
import fauna.net.NetworkHelpers
import fauna.prop.test.{ PropSpec => PS }
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.{ Tables, Value }
import fauna.storage.cassandra.ColumnFamilySchema
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.ops._
import io.netty.buffer.{ ByteBuf, Unpooled }
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.{ MatchResult, Matcher }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._
import org.scalatest.Inside
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

package object test extends NetworkHelpers {

  implicit val ctrl = new ConsoleControl

  val DefaultTimeout = 5.minutes

  def await[T](f: Future[T]): T = Await.result(f, DefaultTimeout)

  implicit class MapVOps(m: MapV) {
    def toData = Data(m)
    def toDiff = Diff(m)
  }

  def setRowTS(ctx: RepoContext, rowKey: ByteBuf, txnTS: Timestamp) =
    ctx.service.storage
      .applyMutationForKey(rowKey) { mut =>
        mut.add(
          Tables.RowTimestamps.CFName,
          Tables.RowTimestamps.encode(txnTS),
          Unpooled.EMPTY_BUFFER,
          txnTS,
          ttl = 0
        )
      }

  def clearRowTS(ctx: RepoContext, rowKey: ByteBuf) =
    ctx.service.storage
      .applyMutationForKey(rowKey) { mut =>
        mut.delete(Tables.RowTimestamps.CFName, Clock.time)
      }

  def insert[K](
    schema: ColumnFamilySchema,
    value: Value[K],
    ttl: Option[Duration] = None): Query[Value[K]] = {
    val k = value.keyPredicate.uncheckedRowKeyBytes
    val c = value.keyPredicate.uncheckedColumnNameBytes
    val write = InsertDataValueWrite(schema.name, k, c, value.data, ttl)
    Query.write(write) map { _ => value }
  }
}

package test {

  import fauna.atoms._

  abstract class PropSpec extends PS(5, 200) with Matchers

  trait RepoMatchers {

    /**
      * Compare two Versions for equality, disregarding their
      * respective transaction times and the distinction between
      * update and create actions.
      *
      * This is convenient for testing Versions regardless of whether
      * their timestamps are fully resolved, e.g.:
      *
      *    AtValid(TS(1)) =~ Resolved(TS(1), ...)
      */
    class EqualVersionMatcher(val expected: Version) extends Matcher[Version] {
      def apply(candidate: Version) = {
        MatchResult(
          expected.parentScopeID.toLong == candidate.parentScopeID.toLong &&
            expected.id == candidate.id &&
            expected.ts.validTS == candidate.ts.validTS &&
            expected.isDeleted == candidate.isDeleted,
          s"$expected did not equal $candidate",
          s"$expected equaled $candidate")
      }
    }

    def equalVersion(expected: Version) = new EqualVersionMatcher(expected)
  }

  trait Spec extends AnyFreeSpec
      with BeforeAndAfter
      with BeforeAndAfterAll
      with OptionValues
      with Eventually
      with PrivateMethodTester
      with Matchers
      with RepoMatchers
      with TryValues
      with Inside {

    implicit override val patienceConfig =
      PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(50, Millis)))
  }

  case class IndexConfig(
    scopeID: ScopeID,
    id: IndexID,
    collectionID: CollectionID,
    terms: Vector[(fauna.repo.TermExtractor, Boolean)] = Vector.empty,
    values: Vector[(fauna.repo.TermExtractor, Boolean)] = Vector.empty,
    constraint: Constraint = Unconstrained,
    isSerial: Boolean = false) extends fauna.repo.IndexConfig {

    val sources = IndexSources(collectionID)

    val binders = { cls: CollectionID =>
      if (cls == collectionID) Some(List.empty) else None
    }
  }

  object DefaultExtractor {
    def apply(path: List[String]) =
      (fauna.repo.IndexConfig.defaultExtractor(path, false), false)
  }
}
