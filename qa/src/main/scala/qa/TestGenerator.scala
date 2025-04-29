package fauna.qa

import fauna.codex.cbor._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.net.http._
import fauna.prop.api.DefaultQueryHelpers
import fauna.prop.Prop
import io.netty.buffer.ByteBuf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * TestGenerator is an encapsulation of a test.
  *
  * Each test must extends TestGenerator and define a schema, an
  * initializer and a stream.
  *
  * The system will create the defined schema and make keys for each
  * Database. The created schema and keys will be passed back to the
  * initializer and stream.
  *
  * An initializer is a RequestIterator run to fill the database
  * with test data.
  *
  * A stream is a RequestStream used to produce traffic against a
  * cluster.
  */
abstract class TestGenerator(
  val name: String,
  val config: QAConfig
) extends DefaultQueryHelpers {
  val expectFailures = false

  private val dbName = Prop.alphaNumString(60, 60) map { n =>
    s"qa-${name}-${n}"
  }

  val log = getLogger()

  // ensure this test generator gets its own instance of propConfig.rand
  implicit val propConfig = config.propConfig.copy()

  // the schema to create before initialization
  def schema: Schema.DB

  // run in sequence in order to initialize instance data
  @annotation.nowarn("cat=unused-params")
  protected def initializer(schema: Schema.DB): RequestIterator =
    RequestIterator.Empty

  // allows for partitioning the initializers in order to run them in parallel
  def initializer(schema: Schema.DB, clientID: Int): RequestIterator =
    if (clientID == 0) {
      initializer(schema)
    } else {
      RequestIterator.Empty
    }

  def stream(schema: Schema.DB): RequestStream

  def streamWithHttp2: Boolean = false

  def useDriver: Boolean = false

  private val RootDB = Schema.DB.Root(config.rootKey)

  /** setup initializes databases, classes, and indexes for this TestGenerator, through the
    *  Core host supplied. */
  def setup(host: CoreNode): Future[Schema.DB] = {
    log.info(s"Running setup for $name on $host")

    val client = QAClient(host)
    def query(authKey: String, body: JSObject): Future[HttpResponse] =
      client.query(FaunaQuery(authKey, body))

    def updateDbTxnTime(db: Schema.DB): Future[Unit] = {
      db.lastSeenTxnTimestamp = Timestamp.ofMicros(client.lastSeenTxnTime)
      Future.unit
    }

    def createSchema(
      parentDB: Schema.DB,
      schema: Vector[Schema]
    ): Future[Vector[Schema]] =
      schema match {
        case schemas if schemas.size > 1 =>
          schemas.foldLeft(Future.successful(Vector.empty[Schema])) {
            case (accF, entry) =>
              accF flatMap { a =>
                createSchema(parentDB, Vector(entry)) map { _ ++ a }
              }
          }
        case Vector(db: Schema.DB)          => createDB(parentDB, db)
        case Vector(cls: Schema.Collection) => createCollection(parentDB, cls)
        case Vector(idx: Schema.Index)      => createIndex(parentDB, idx)
        case vec                            => Future.successful(vec)
      }

    def createDB(
      parentDB: Schema.DB,
      db: Schema.DB,
      container: Boolean = false,
      account: Option[Long] = None): Future[Vector[Schema.DB]] =
      for {
        dbRes <- query(
          parentDB.adminKey.get,
          CreateDatabase(
            MkObject(
              "name" -> db.name,
              "priority" -> db.priority,
              "container" -> container,
              "account" -> MkObject(
                "id" -> account.fold(JSNull: JSValue) { JS(_) }
              )
            )
          )
        )
        adminKey <- createKey(parentDB, db.name, "admin")
        serverKey <- createKey(parentDB, db.name, "server")
        newDB = Schema.DB(
          db.name,
          db.priority,
          Vector.empty,
          Some(adminKey),
          Some(serverKey)
        )
        _ = dbRes.body.maybeRelease()
        _ = log.info(s"Created DB: $newDB")
        schema <- createSchema(newDB, db.schema)
        _ = updateDbTxnTime(newDB)
      } yield Vector(newDB.copy(schema = schema))

    def createCollection(
      parentDB: Schema.DB,
      collection: Schema.Collection
    ): Future[Vector[Schema.Collection]] =
      for {
        resp <- query(
          parentDB.serverKey.get,
          CreateCollection(
            MkObject(
              "name" -> collection.name,
              "history_days" -> collection.historyDays.getOrElse(0L)))
        )
        _ = resp.body.maybeRelease()
        _ = log.info(s"Created collection: $collection")
      } yield Vector(collection)

    def createIndex(
      parentDB: Schema.DB,
      idx: Schema.Index
    ): Future[Vector[Schema.Index]] = {
      val terms = idx.terms map {
        case Schema.Index.Term(field, transform) =>
          MkObject(
            "field" -> JSArray(field map { JSString(_) }: _*),
            "transform" -> transform
          )
      }

      val values = idx.values map {
        case Schema.Index.Value(field, reverse, transform) =>
          MkObject(
            "field" -> JSArray(field map { JSString(_) }: _*),
            "reverse" -> reverse,
            "transform" -> transform
          )
      }

      val obj = MkObject(
        "name" -> idx.name,
        "source" -> Ref(s"classes/${idx.source}"),
        "unique" -> idx.unique,
        "active" -> true,
        "terms" -> JSArray(terms: _*),
        "values" -> JSArray(values: _*)
      )

      val key = parentDB.serverKey.get

      for {
        resp <- query(key, CreateF(Ref("indexes"), obj))
        json <- resp.body.data map { JSON.parse[JSValue](_).as[JSObject] }
        _ = resp.body.maybeRelease()
        ref = (json / "resource" / "ref").as[JSObject]
        _ = log.info(s"Created index: $idx")
      } yield Vector(idx)
    }

    def createKey(parentDB: Schema.DB, db: String, role: String): Future[String] = {
      val obj = MkObject("role" -> role, "database" -> Ref(s"databases/${db}"))
      for {
        resp <- query(parentDB.adminKey.get, CreateF(Ref("keys"), obj))
        data <- resp.body.data
        secret = (JSON
          .tryParse[JSValue](data)
          .getOrElse(JSNull) / "resource" / "secret").as[String]
        _ = resp.body.maybeRelease()
        _ = log.info(s"Created key: $db/$role $secret")
      } yield secret
    }

    for {
      idRes <- query(RootDB.adminKey.get, NewID())
      data <- idRes.body.data
      id = (JSON
        .tryParse[JSValue](data)
        .getOrElse(JSNull) / "resource").as[String].toLong
      _ = idRes.body.maybeRelease()
      parent <- createDB(
        RootDB,
        Schema.DB(dbName.sample),
        container = true,
        account = Some(id))
      // If query ops limits enabled per-tenant, need to set limits on the account object
      resp <- query(
                RootDB.adminKey.get,
                If(config.enableQueryLimitsPerTenant,
                  Update(
                    Ref(s"databases/${parent.head.name}"),
                    MkObject(
                      "account" -> MkObject(
                        "id" -> id,
                        "limits" -> MkObject(
                          "read_ops" -> MkObject(
                            "hard" -> config.queryReadLimitPerTenant),
                          "write_ops" -> MkObject(
                            "hard" -> config.queryWriteLimitPerTenant),
                          "compute_ops" -> MkObject(
                            "hard" -> config.queryComputeLimitPerTenant)
                        )
                      )
                    )
                  ),
                  JSNull
                )
              )
      _ = resp.body.maybeRelease()
      db <- createDB(parent.head, schema)
    } yield db.head
  }

  @annotation.nowarn("cat=unused-params")
  def tearDown(db: Schema.DB, host: CoreNode): Future[Unit] = {
    Future.unit
  }
}

/**
  * A marker trait for TestGenerators that are configured to validate
  * database state.
  *
  * 'getValidateStateQuery' should return a query that runs At() the provided
  * Timestamp (to ensure consistent reads across nodes) and if the query
  * tests any invariants, it should Abort() and provide debugging information
  * in the Abort message. See JepsenBank.scala for an example.
  */
trait ValidatingTestGenerator {
  def getValidateStateQuery(schema: Schema.DB, ts: Timestamp): FaunaQuery

  // Whether validate query expects failure; override for "negative" tests
  val expectValidateQueryToFail: Boolean = false

  // Whether validate query should run using At() and passing both "now" and
  // a random time since the beginning of the test run; override for testgens
  // where temporality of the query isn't interesting
  val isTimeAware: Boolean = true

  // Whether validate query should return a simple Pass/Fail result; override
  // for queries that use random sampling data where different workers would
  // see different results
  val isPassFail: Boolean = false
}

/**
  * A serializer for TestGenerators.
  *
  * Since TestGenerators are Prop based the same QAConfig should produce
  * the same example data. Thus, we need only serialize the config and re-
  * instantiate the TestGenerator with the given config in order to get
  * identical data on each receiver.
  */
object TestGenerator {
  private case class SerializedTestGen(clazz: String, name: String, config: QAConfig)
  private implicit val serializedLGCodec = CBOR.RecordCodec[SerializedTestGen]

  implicit val TestGeneratorCodec = CBOR.AliasCodec[TestGenerator, ByteBuf](
    { buf =>
      val SerializedTestGen(className, name, config) =
        CBOR.decode[SerializedTestGen](buf)
      val clazz = Class.forName(className)
      val ctor = clazz.getDeclaredConstructor(classOf[String], classOf[QAConfig])
      ctor.newInstance(name, config).asInstanceOf[TestGenerator]
    }, { lg =>
      CBOR.encode(SerializedTestGen(lg.getClass.getName, lg.name, lg.config))
    }
  )
}
