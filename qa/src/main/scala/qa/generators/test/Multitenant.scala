package fauna.qa.generators.test

import fauna.qa._
import fauna.codex.json.JSObject
import fauna.prop._
import fauna.prop.api._

/**
  * Simulate a multitenant workload. A tenant hierarchy is created with varying
  * priorities. Then runs a read workload against each tenant. Priorities can be
  * configured to be updated during the course of the run.
  * Config fields:
  *  - multitenant.data.maxInstances = number of instances to initially create
  *  - multitenant.test.priority-changes = the amount of time to wait between
  *  priority updates
  */
class MultitenantGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with JSGenerators {

  val maxInstances = config.getInt("multitenant.data.maxInstances")
  val instsPerInit = maxInstances / config.clients

  val schema = Schema.DB(
    "multitenant",
    Schema.DB(
      "tenant-1",
      Schema.Collection("aclass"),
      Schema.Index("aclass_by_field", "aclass", Vector("data", "field"))
    ),
    Schema.DB(
      "tenant-2",
      Schema.DB(
        "tenant-2-1",
        20,
        Schema.DB(
          "tenant-2-1-1",
          1,
          Schema.Collection("aclass"),
          Schema.Index("aclass_by_field", "aclass", Vector("data", "field"))
        ),
        Schema.DB(
          "tenant-2-1-2",
          100,
          Schema.Collection("aclass"),
          Schema.Index("aclass_by_field", "aclass", Vector("data", "field"))
        )
      ),
      Schema.DB(
        "tenant-2-2",
        20,
        Schema.Collection("aclass"),
        Schema.Index("aclass_by_field", "aclass", Vector("data", "field"))
      ),
      Schema.DB(
        "tenant-2-3",
        50,
        Schema.Collection("aclass"),
        Schema.Index("aclass_by_field", "aclass", Vector("data", "field"))
      )
    )
  )

  val dataP = for {
    obj <- jsObject(1, 20)
    dataP = jsObject("field" -> jsBoolean) map { _ ++ obj }
    data <- jsObject("data" -> dataP)
  } yield data

  def instances(schema: Schema.DB, cls: String) = {
    val auth = schema.serverKey.get
    val resP = dataP map { DBResource(auth, Ref(s"classes/${cls}"), _) }
    resP times (instsPerInit) sample
  }

  private def initSchema(db: Schema.DB, schema: Schema): List[DBResource] =
    schema match {
      case db: Schema.DB =>
        db.schema.foldLeft(List.empty[DBResource]) {
          case (acc, schema) => acc ::: initSchema(db, schema)
        }
      case Schema.Collection(name, _) => instances(db, name).toList
      case _                          => List.empty
    }

  override def initializer(db: Schema.DB, clientID: Int) =
    RequestIterator("generate-data", initSchema(db, db).map(_.toRequest))

  def readQuery(auth: String, index: String, cursor: JSObject): FaunaQuery = {
    val query =
      MapF(
        Lambda("ref" -> Get(Var("ref"))),
        Paginate(Match(Ref(s"indexes/${index}"), true)) ++ cursor
      )

    new FaunaQuery(auth, query, Some(CursorDir.After)) {
      override def nextPage(cursor: JSObject) = readQuery(auth, index, cursor)
    }
  }

  def readStreams(db: Schema.DB, schema: Schema): List[RequestStream] =
    schema match {
      case db: Schema.DB =>
        db.schema.foldLeft(List.empty[RequestStream]) {
          case (acc, schema) => acc ::: readStreams(db, schema)
        }
      case idx: Schema.Index =>
        List(
          RequestStream(
            s"${db.name}-reads",
            () => readQuery(db.serverKey.get, idx.name, JSObject.empty)
          )
        )
      case _ => List.empty
    }

  def writeStreams(db: Schema.DB, schema: Schema): List[RequestStream] =
    schema match {
      case db: Schema.DB =>
        db.schema.foldLeft(List.empty[RequestStream]) {
          case (acc, schema) => acc ::: writeStreams(db, schema)
        }
      case Schema.Collection(cls, _) =>
        List(
          RequestStream(
            s"${db.name}-writes",
            () =>
              FaunaQuery(
                db.serverKey.get,
                CreateF(Ref(s"classes/$cls"), Quote(dataP.sample))
              )
          )
        )
      case _ => List.empty
    }

  def updatePriorities(schema: Vector[Schema], key: String): FaunaQuery = {
    def mkQueries(schema: Schema, key: String): List[FaunaQuery] =
      schema match {
        case db: Schema.DB =>
          val q = FaunaQuery(
            key,
            Update(
              Ref(s"databases/${db.name}"),
              MkObject("priority" -> Prop.int(1 to 500).sample)
            )
          )
          db.schema.foldLeft(List(q)) {
            case (qs, schema) => qs ::: mkQueries(schema, db.adminKey.get)
          }
        case _ => List.empty[FaunaQuery]
      }

    val queries = schema.foldLeft(List.empty[FaunaQuery]) {
      case (a, s) => a ::: mkQueries(s, key)
    }

    queries.reduceLeft[FaunaQuery] {
      case (b, a) =>
        FaunaQuery.multi(a.authKey, a, { _: JSObject =>
          Some(b)
        })
    }
  }

  def stream(db: Schema.DB) =
    new MultiStream((readStreams(db, db) ++: writeStreams(db, db)).toIndexedSeq)
}

class Multitenant(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new MultitenantGenerator("multitenant", config)
  )
}
