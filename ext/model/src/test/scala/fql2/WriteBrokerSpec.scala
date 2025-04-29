package fauna.model.test

import fauna.atoms._
import fauna.lang.syntax.Op
import fauna.lang.Timestamp
import fauna.model.runtime.fql2._
import fauna.model.schema.{ CollectionConfig, Wildcard }
import fauna.repo.query.Query
import fauna.repo.schema.{
  ConstraintFailure,
  DataMode,
  DocIDSource,
  FieldSchema,
  Path
}
import fauna.repo.schema.migration.MigrationList
import fauna.repo.schema.ScalarType
import fauna.repo.values.Value
import fauna.storage.doc.Field
import fql.ast.{ Span, Src }
import scala.collection.immutable.SeqMap
import scala.concurrent.duration._

class WriteBrokerSpec extends FQL2Spec {

  "WriteBroker" - {
    "schedules revalidation hooks" in {
      val auth = newAuth
      val span = Span(42, 24, Src.Null)
      val intp = new FQLInterpreter(auth).withStackFrame(span)

      val failures =
        Seq(
          ConstraintFailure
            .ValidatorFailure(Path.Root, "fail"))

      val broker =
        new WriteBroker(
          CollectionConfig(
            parentScopeID = auth.scopeID,
            name = "TestColl",
            id = UserCollectionID.MinValue,
            revalidationHooks = List(PostEvalHook.Revalidation("foo") { (_, _, _) =>
              Query.value(failures)
            }),
            // mocks
            nativeIndexes = Nil,
            indexConfigs = Nil,
            collIndexes = List.empty,
            checkConstraints = List.empty,
            historyDuration = Duration.Inf,
            ttlDuration = Duration.Inf,
            documentTTLs = true,
            minValidTimeFloor = Timestamp.Epoch,
            fields = Map.empty,
            computedFields = Map.empty,
            computedFieldSigs = SeqMap.empty,
            definedFields = Map.empty,
            wildcard = Some(Wildcard.any("TestColl")),
            idSource = DocIDSource.Sequential(1, 10),
            writeHooks = Nil,
            internalMigrations = MigrationList.empty,
            internalStagedMigrations = None
          ))

      val res = ctx ! {
        // Schedules revalidation
        broker.createDocument(
          intp,
          DataMode.Default,
          Value.Struct.empty
        ) flatMapT { _ =>
          // When executed later, fails with revalidation errors
          intp.runPostEvalHooks()
        }
      }

      res should matchPattern {
        case Result.Err(
              QueryRuntimeFailure.Simple(
                "constraint_failure",
                "Failed to create document in collection `TestColl`.",
                `span`,
                `failures`)
            ) =>
      }
    }

    "alias fields are renamed on storage" in {
      val auth = newAuth

      val collId = CollectionID(1)

      val config = new CollectionConfig(
        parentScopeID = auth.scopeID,
        name = "Person",
        id = collId,
        nativeIndexes = List.empty,
        indexConfigs = List.empty,
        collIndexes = List.empty,
        checkConstraints = List.empty,
        historyDuration = Duration.Inf,
        ttlDuration = Duration.Inf,
        documentTTLs = true,
        minValidTimeFloor = Timestamp.Epoch,
        fields = Map(
          "userField" -> FieldSchema(ScalarType.Str, aliasTo = Some("storageField"))
        ),
        computedFields = Map.empty,
        computedFieldSigs = SeqMap.empty,
        definedFields = Map.empty,
        wildcard = None,
        idSource = DocIDSource.Sequential(
          1,
          10
        ),
        writeHooks = Nil,
        revalidationHooks = Nil,
        internalMigrations = MigrationList.empty,
        internalStagedMigrations = None
      )

      val broker = new WriteBroker(config)

      val version = (ctx ! broker.createDocument(
        new FQLInterpreter(auth),
        DataMode.Default,
        Value.Struct("userField" -> Value.Str("Foo"))
      )).getOrElse(fail())

      version.data(Field[Option[String]]("userField")) shouldBe empty
      version.data(Field[Option[String]]("storageField")) shouldBe Some("Foo")
    }
  }
}
