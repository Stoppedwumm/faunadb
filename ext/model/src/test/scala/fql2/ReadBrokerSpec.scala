package fauna.model.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{ FQLInterpreter, ReadBroker }
import fauna.model.schema.{ CollectionConfig, Wildcard }
import fauna.repo.query.ReadCache
import fauna.repo.schema.{ DocIDSource, FieldSchema, ScalarType, SchemaType }
import fauna.repo.schema.migration.MigrationList
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.{ Data, Field }
import fql.ast.{ Name, Span }
import scala.collection.immutable.SeqMap
import scala.concurrent.duration._

class ReadBrokerSpec extends FQL2Spec {

  var auth: Auth = _
  var interp: FQLInterpreter = _

  before {
    auth = newDB
    interp = new FQLInterpreter(auth)
  }

  def field(name: String) =
    Name(name, Span.Null)

  def collConfig(
    scope: ScopeID,
    collectionID: CollectionID,
    aliasField: Option[String] = None) = {

    new CollectionConfig(
      parentScopeID = scope,
      name = "testCollection",
      id = collectionID,
      nativeIndexes = List.empty,
      indexConfigs = List.empty,
      collIndexes = List.empty,
      checkConstraints = List.empty,
      historyDuration = Duration.Inf,
      ttlDuration = Duration.Inf,
      documentTTLs = true,
      minValidTimeFloor = Timestamp.Epoch,
      fields = Map(
        "hideMe" -> FieldSchema(ScalarType.Str, internal = true),
        "name" -> FieldSchema(ScalarType.Str),
        "userField" -> FieldSchema(
          SchemaType.Optional(ScalarType.Str),
          aliasTo = aliasField)
      ),
      computedFields = Map.empty,
      computedFieldSigs = SeqMap.empty,
      definedFields = Map.empty,
      // this ensures we trigger the wild card field addition path in ReadBroker
      wildcard = Some(Wildcard.any("testCollection")),
      idSource = DocIDSource.Sequential(1, 10),
      writeHooks = Nil,
      revalidationHooks = Nil,
      internalMigrations = MigrationList.empty,
      internalStagedMigrations = None
    )
  }

  "ReadBroker" - {

    "internal fields are omitted from getAllFields" in {
      val collId = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val personDoc = evalOk(
        auth,
        """|Person.create({
           |  name: "John",
           |  hideMe: "hello"
           |})""".stripMargin
      ).to[Value.Doc]

      val broker = new ReadBroker(collConfig(auth.scopeID, collId))
      val res =
        (ctx ! broker.getAllFields(interp, personDoc))
          .getOrElse(fail())

      (res / "name").as[String] shouldEqual "John"
      (res / "hideMe").asOpt[String] shouldBe empty
    }

    "internal fields are omitted from getField" in {
      val collId = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val personDoc = evalOk(
        auth,
        """|Person.create({
           |  name: "John",
           |  hideMe: "hello"
           |})""".stripMargin
      ).to[Value.Doc]

      val broker = new ReadBroker(collConfig(auth.scopeID, collId))
      val normalField = ctx ! broker.getField(interp, personDoc, field("name"))
      val internalField = ctx ! broker.getField(interp, personDoc, field("hideMe"))
      normalField.unsafeGet.as[String] shouldEqual "John"
      internalField.unsafeGet shouldBe a[Value.Null]
    }

    "alias fields are renamed on getAllFields" in {
      val collId = CollectionID(1)
      val personDoc = DocID(SubID(1), collId)
      val config =
        collConfig(
          auth.scopeID,
          collId,
          aliasField = Some("storageField")
        )

      ctx ! Store.create(
        config.Schema.internalSchema,
        Data(
          Field[String]("name") -> "John",
          Field[String]("hideMe") -> "hideMe",
          Field[String]("userField") -> "Foo"
        ))

      val version = (ctx ! Store.getUnmigrated(auth.scopeID, personDoc)).value
      version.data(Field[String]("storageField")) shouldBe "Foo"

      val broker = new ReadBroker(config)

      val res =
        (ctx ! broker.getAllFields(interp, Value.Doc(personDoc)))
          .getOrElse(fail())

      (res / "userField").as[String] shouldBe "Foo"
    }

    "alias fields are renamed on getField" in {
      val collId = CollectionID(1)
      val personDoc = DocID(SubID(1), collId)
      val doc = Value.Doc(personDoc)
      val config =
        collConfig(
          auth.scopeID,
          collId,
          aliasField = Some("storageField")
        )

      ctx ! Store.create(
        config.Schema.internalSchema,
        Data(
          Field[String]("name") -> "John",
          Field[String]("hideMe") -> "hideMe",
          Field[String]("userField") -> "Foo"
        ))

      val version = (ctx ! Store.getUnmigrated(auth.scopeID, personDoc)).value
      version.data(Field[String]("storageField")) shouldBe "Foo"

      val broker = new ReadBroker(config)
      val aliasField = ctx ! broker.getField(interp, doc, field("userField"))
      aliasField.unsafeGet.as[String] shouldBe "Foo"
    }

    "Partials" - {

      def scopeID = auth.scopeID
      val prefix = ReadCache.Prefix.empty
      val collID = CollectionID(1024)
      val docID = DocID(SubID(1), collID)
      val doc = Value.Doc(docID)

      val dummySrcHint =
        ReadCache.CachedDoc.SetSrcHint(
          ScopeID.MaxValue,
          IndexID.MaxValue,
          "dummy",
          CollectionID.MaxValue,
          "dummy",
          Seq.empty,
          Span.Null
        )

      def write(tuple: (String, String)) =
        Store.insertUnmigrated(
          scopeID,
          docID,
          Data(
            Field[Data]("data") ->
              Data(
                Field[String](tuple._1) -> tuple._2
              ))
        )

      "return scalars from partials" in {
        val config = collConfig(auth.scopeID, collID, aliasField = Some("foo"))
        val broker = new ReadBroker(config)

        ctx ! {
          for {
            _ <- Store.feedPartial(
              dummySrcHint,
              prefix,
              scopeID,
              docID,
              validTS = None,
              partials = Map(
                ("ts" :: Nil) -> Value.Time(Timestamp.Epoch),
                ("data" :: "foo" :: Nil) -> Value.Str("bar")
              )
            )
            ts       <- broker.getField(interp, doc, field("ts"))
            foo      <- broker.getField(interp, doc, field("foo"))
            user     <- broker.getField(interp, doc, field("userField"))
            notFound <- broker.getField(interp, doc, field("*not-found*"))
          } yield {
            ts.unsafeGet shouldBe Value.Time(Timestamp.Epoch)
            foo.unsafeGet shouldBe Value.Str("bar")
            user.unsafeGet shouldBe Value.Str("bar")
            notFound.unsafeGet shouldBe
              Value.Null.missingField(doc, field("*not-found*"))
          }
        }
      }

      "return partial values from partials" in {
        val config = collConfig(auth.scopeID, collID)
        val broker = new ReadBroker(config)

        ctx ! {
          for {
            _ <- Store.feedPartial(
              dummySrcHint,
              prefix,
              scopeID,
              docID,
              validTS = None,
              partials = Map(
                ("data" :: "foo" :: Nil) -> Value.Str("bar")
              )
            )
            data <- broker.getField(interp, doc, field("data"))
          } yield {
            inside(data.unsafeGet) { case p: Value.Struct.Partial =>
              inside(p.fragment.project("foo" :: Nil).value) {
                case v: ReadCache.Fragment.Value =>
                  v.unwrap shouldBe Value.Str("bar")
              }
            }
          }
        }
      }

      "fetches underlying version on insufficint partials" in {
        val config = collConfig(auth.scopeID, collID)
        val broker = new ReadBroker(config)
        ctx ! write("foo" -> "bar")

        ctx ! {
          for {
            _ <- Store.feedPartial(
              dummySrcHint,
              prefix,
              scopeID,
              docID,
              validTS = None,
              partials = Map(
                ("ts" :: Nil) -> Value.Time(Timestamp.Epoch)
              )
            )
            data <- broker.getField(interp, doc, field("data"))
          } yield {
            data.unsafeGet shouldBe Value.Struct("foo" -> Value.Str("bar"))
          }
        }
      }

      "fetches underlying version at the correct write prefix" in {
        val config = collConfig(auth.scopeID, collID)
        val broker = new ReadBroker(config)

        ctx ! {
          for {
            _      <- write("fizz" -> "buzz")
            _      <- write("foo" -> "bar")
            prefix <- Store.writePrefix(scopeID, docID)
            _ <- Store.feedPartial(
              dummySrcHint,
              prefix,
              scopeID,
              docID,
              validTS = None,
              partials = Map(
                ("data" :: "fizz" :: Nil) -> Value.Str("buzz")
              )
            )
            data <- broker.getField(interp, doc, field("data"))
            _    <- write("foo" -> "bazz")
            partial = data.unsafeGet.asInstanceOf[Value.Struct.Partial]
            full <- broker.materializePartial(partial, allowTSField = true)
          } yield {
            (full.value / "foo") shouldBe Value.Str("bar") // not bazz
          }
        }
      }
    }
  }
}
