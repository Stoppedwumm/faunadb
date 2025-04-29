package benchmarks.model

import fauna.atoms._
import fauna.model.runtime.fql2.serialization.{ FQL2ValueMaterializer, ValueFormat }
import fauna.model.schema.CollectionConfig
import fauna.model.tasks.export.VersionFormatter
import fauna.model.Cache
import fauna.repo.doc._
import fauna.repo.values.Value
import fauna.storage.doc._
import fauna.storage.ir._
import org.openjdk.jmh.annotations._
import FQL2Bench._

@Fork(1)
@State(Scope.Benchmark)
class FQL2MaterializeBench {

  val coll1 = FQL2Bench.dummyCollection(ScopeID.RootID, CollectionID(1024), "Foo")
  val coll2 = FQL2Bench.dummyCollection(ScopeID.RootID, CollectionID(1024), "Bar")

  val doc1 = {
    val fields = (1 to 1000).map(i => i.toString -> StringV("aa" * i))
    dummyDoc(coll1, 123, MapV(fields: _*))
  }
  val doc2 = {
    val fields1 = (1 to 500).map(i => i.toString -> LongV(i))
    val fields2 = (501 to 1000).map(i => i.toString -> StringV("xx" * i))
    val fields3 =
      (1001 to 1500).map(i => i.toString -> DocIDV(DocID(SubID(i), coll1.id)))
    val fields = fields1 ++ fields2 ++ fields3
    dummyDoc(coll2, 456, MapV(fields: _*))
  }

  ctx ! Cache.cacheCollectionForTesting(coll1)
  ctx ! Cache.cacheCollectionForTesting(coll2)

  val collections = Seq(coll1, coll2).map(c => c.id -> c).toMap
  val formatter = new VersionFormatter(collections, ValueFormat.Tagged)

  @Benchmark
  def materialize() = {
    ctx ! FQL2ValueMaterializer.materialize(intp, doc2)
  }

  @Benchmark
  def exportTaskFormat() = {
    val (_, buf) = ctx ! formatter.formatVersion(doc2.versionOverride.get)
    buf.release()
  }

  def dummyDoc(coll: CollectionConfig, id: Long, data: MapV) = {
    val vers = Version.Live(
      ScopeID.RootID,
      DocID(SubID(id), coll.id),
      SchemaVersion.Min,
      Data(MapV("data" -> data)))
    Value.Doc(vers.id, versionOverride = Some(vers))
  }
}
