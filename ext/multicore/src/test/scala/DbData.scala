package fauna.multicore.test

import fauna.codex.json.{ JSArray, JSObject, JSValue }
import fauna.net.http.{ HttpClient, HttpResponse }
import fauna.prop.{ Prop, PropConfig }
import fauna.prop.api.{ APIGenerators, Collection, Database, DefaultQueryHelpers, Index }
import scala.concurrent.Future

object DbData {
  def apply(aDatabase: Prop[Database], classCount: Int, instanceCount: Int, fieldCount: Int, gen: APIGenerators)(implicit pconf: PropConfig): Prop[DbData] =
    aDatabase map { db =>
      DbData(
        db,
        gen.aCollection(db)
          .map({ cls =>
            val indices = gen.aUniqueName
              .map({ name =>
                IndexData(
                  name,
                  gen.anIndex(cls, Prop.const(Seq(JSArray("data", name)))).sample)
              })
              .times(fieldCount)
              .sample

            val b = JSObject.newBuilder
            val instances = (1 to instanceCount) map { _ =>
              indices foreach { idx =>
                b += ((idx.field, gen.jsAlphaNumString(15, 30).sample))
              }
              val inst = gen.aDocument(cls, dataProp = Prop.const(b.result())).sample
              b.clear()
              inst
            }
            CollectionData(cls, indices, instances)
          })
          .times(classCount)
          .sample
      )
    }
}

case object DataIntegrityVerifier
  extends Spec
    with DefaultQueryHelpers {

  def verify(dbData: DbData, trace: Boolean, api: HttpClient): Unit = {
    val dbkey = dbData.db.key
    val qf: JSObject => Future[HttpResponse] =
      if (trace) { (query) =>
        api.trace(query, dbkey)
      } else { (query) =>
        api.query(query, dbkey)
      }

    withClue(s"dbkey=$dbkey") {
      dbData.classes foreach { cls =>
        cls.instances foreach { inst =>
          withClue(s"inst=$inst") {
            val instRefObj = inst.refObj
            // Check that every instance is indexed properly
            cls.indices foreach { idx =>
              val idxRefObj = idx.index.refObj
              withClue(s"idxRef=$idxRefObj") {
                val query = Paginate(Match(idxRefObj, inst / "data" / idx.field))
                withClue(s"query=$query") {
                  val res = qf(query)
                  res should respond(OK)
                  withClue(s"res=${res.json}") {
                    ((res.json / "resource" / "data")
                      .as[Seq[JSValue]] contains instRefObj) should equal(true)
                  }
                }
              }

              // Check that every instance can be retrieved
              val get = qf(Get(inst.refObj))
              get should respond(OK)
              (get.json / "resource" / "data").as[JSObject] should equal(
                inst / "data")
            }
          }
        }
      }
    }
  }
}

// Used to keep an in-memory representation of a FaunaDB database.
// Useful when building a database to check the integrity of the data
case class DbData(db: Database, classes: Seq[CollectionData]) {

  def verifyDataIntegrity(trace: Boolean, api: HttpClient): Unit =
    DataIntegrityVerifier.verify(this, trace, api)
}

case class CollectionData(
  collection: Collection,
  indices: Seq[IndexData],
  instances: Seq[JSObject])

case class IndexData(field: String, index: Index)


