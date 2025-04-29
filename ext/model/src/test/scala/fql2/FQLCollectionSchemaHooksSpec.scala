package fauna.model.test

import fauna.atoms._
import fauna.model._
import fauna.model.schema._
import fauna.model.schema.index._
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.storage.index.IndexSources

class FQLCollectionSchemaHooksSpec extends FQL2WithV4Spec {
  "BackingIndexManager" - {
    "update status & queryable" in {
      val auth = newDB

      val collID = mkColl(auth, "Person")

      (1 to Index.BuildSyncSize) foreach { i =>
        evalOk(
          auth,
          s"""|[
              |  Person.create({foo: ${i * 2 + 0}}),
              |  Person.create({foo: ${i * 2 + 1}})
              |]""".stripMargin
        )
      }

      evalOk(
        auth,
        """|Person.definition.update({
           |  indexes: {
           |    byName: {
           |      terms: [{field: "name"}]
           |    }
           |  }
           |})
           |""".stripMargin
      )

      val coll =
        (ctx ! Collection.getUncached(auth.scopeID, collID)).value.active.get
      val id = coll.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .find(_.name == "byName")
        .value
        .indexID

      evalOk(
        auth,
        "Person.definition.indexes.byName?.status"
      ).as[String] shouldBe CollectionIndex.Status.Building.asStr

      evalOk(
        auth,
        "Person.definition.indexes.byName?.queryable"
      ).as[Boolean] shouldBe false

      (ctx ! CollectionSchemaHooks.markComplete(auth.scopeID, id)) shouldBe true

      evalOk(
        auth,
        "Person.definition.indexes.byName?.status"
      ).as[String] shouldBe CollectionIndex.Status.Complete.asStr

      evalOk(
        auth,
        "Person.definition.indexes.byName?.queryable"
      ).as[Boolean] shouldBe true

      (ctx ! CollectionSchemaHooks.markFailed(auth.scopeID, id)) shouldBe true

      evalOk(
        auth,
        "Person.definition.indexes.byName?.status"
      ).as[String] shouldBe CollectionIndex.Status.Failed.asStr

      evalOk(
        auth,
        "Person.definition.indexes.byName?.queryable"
      ).as[Boolean] shouldBe false
    }

    "don't update non FQL-X indexes" in {
      val auth = newDB

      mkColl(auth, "Person")

      evalV4Ok(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "Person.byName",
            "source" -> ClsRefV("Person")
          ))
      )

      val id =
        (ctx ! Index.idByName(auth.scopeID, "Person.byName")).value

      (ctx ! CollectionSchemaHooks.markFailed(auth.scopeID, id)) shouldBe false
    }

    "update multiple collection indexes state" in {
      val auth = newDB

      val collID = mkColl(auth, "Person")

      val id1 = UserIndexID.MinValue
      val id2 = IndexID(id1.toLong + 1)

      val collData = Data(
        Field[String]("name") -> "Person",
        Field[Vector[Data]]("backingIndexes") -> Vector(
          Data(
            Field[IndexID]("indexID") -> id1,
            Field.ZeroOrMore[Data]("terms") -> Vector(
              Data(
                Field[String]("field") -> "name"
              )
            ),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr,
            Field[Boolean]("queryable") -> false
          ),
          Data(
            Field[IndexID]("indexID") -> id2,
            Field.ZeroOrMore[Data]("terms") -> Vector(
              Data(
                Field[String]("field") -> "name"
              )
            ),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr,
            Field[Boolean]("queryable") -> false
          )
        ),
        Field[Data]("indexes") -> Data(
          Field[Data]("byName") -> Data(
            Field.ZeroOrMore[Data]("terms") -> Vector(
              Data(
                Field[String]("field") -> "name"
              )
            ),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr,
            Field[Boolean]("queryable") -> false
          ),
          Field[Data]("byNameSorted") -> Data(
            Field.ZeroOrMore[Data]("terms") -> Vector(
              Data(
                Field[String]("field") -> "name"
              )
            ),
            Field[String]("status") -> CollectionIndex.Status.Building.asStr,
            Field[Boolean]("queryable") -> false
          )
        )
      )

      ctx ! {
        Store.getUnmigrated(auth.scopeID, collID.toDocID) flatMap { vOpt =>
          val data = vOpt.get.data.patch(Diff(collData.fields))
          SchemaCollection.Collection(auth.scopeID).insert(collID, data)
        }
      }

      val coll0 =
        (ctx ! Collection.getUncached(auth.scopeID, collID)).value.active.get
      coll0.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .find(_.name == "byName")
        .value
        .queryable shouldBe false
      coll0.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .find(_.name == "byNameSorted")
        .value
        .queryable shouldBe false

      val idxData = Data(
        Index.IndexByField -> Vector.empty,
        Index.CoveredField -> Vector.empty,
        Index.SourceField -> Vector(SourceConfig(IndexSources(collID))),
        SchemaNames.NameField -> SchemaNames.Name("Person.byName"),
        Index.ActiveField -> false,
        Index.CollectionIndexField -> Some(true)
      )

      ctx ! SchemaCollection.Index(auth.scopeID).insert(id1, idxData)

      ctx ! CollectionSchemaHooks.markComplete(auth.scopeID, id1) shouldBe true

      val coll1 =
        (ctx ! Collection.getUncached(auth.scopeID, collID)).value.active.get
      coll1.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .find(_.name == "byName")
        .value
        .queryable shouldBe true
      coll1.collIndexes
        .asInstanceOf[List[UserDefinedIndex]]
        .find(_.name == "byNameSorted")
        .value
        .queryable shouldBe true
    }

    "fail to update collection index state" in {
      val auth = newDB

      // creates a collection that doesn't have any index on
      // its configs and a collection index where the
      // source is the collection.
      val collID = mkColl(auth, "Person")

      val idxData = Data(
        Index.IndexByField -> Vector.empty,
        Index.CoveredField -> Vector.empty,
        Index.SourceField -> Vector(SourceConfig(IndexSources(collID))),
        SchemaNames.NameField -> SchemaNames.Name("Person.byName"),
        Index.ActiveField -> false,
        Index.CollectionIndexField -> Some(true)
      )

      ctx ! SchemaCollection.Index(auth.scopeID).insert(IndexID(32768), idxData)

      the[IllegalStateException] thrownBy {
        ctx ! CollectionSchemaHooks.markComplete(auth.scopeID, IndexID(32768))
      } should have message "Unable to find backing index for indexID: IndexID(32768) on collection: CollectionID(1024)"
    }

    "hooks" - {
      "creates indexes on create" in {
        val auth = newDB

        val collID = CollectionID(1024)

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 0

        evalOk(
          auth,
          """|Collection.create({
             |  name: "Person",
             |  indexes: {
             |    byName: {
             |      terms: [{ field: "name" }]
             |    },
             |    byNameSorted: {
             |      terms: [{ field: "name" }],
             |      values: [{ field: "name" }]
             |    }
             |  }
             |})""".stripMargin
        )

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 2
      }

      "creates indexes on update" in {
        val auth = newDB

        val collID = evalOk(
          auth,
          """|Collection.create({
             |  name: "Person"
             |})""".stripMargin
        ).to[Value.Doc].id.as[CollectionID]

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 0

        evalOk(
          auth,
          """|Collection.byName("Person")!.update({
             |  indexes: {
             |    byName: {
             |      terms: [{ field: "name" }]
             |    },
             |    byNameSorted: {
             |      terms: [{ field: "name" }],
             |      values: [{ field: "name" }]
             |    }
             |  }
             |})""".stripMargin
        )

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 2
      }

      "removes indexes on delete" in {
        val auth = newDB

        val collID = evalOk(
          auth,
          """|Collection.create({
             |  name: "Person",
             |  indexes: {
             |    byName: {
             |      terms: [{ field: "name" }]
             |    }
             |  }
             |})""".stripMargin
        ).to[Value.Doc].id.as[CollectionID]

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 1

        evalV4Ok(
          auth,
          CreateIndex(
            MkObject(
              "name" -> "Person.byName",
              "source" -> ClsRefV("Person")
            ))
        )

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 2

        evalOk(auth, "Collection.byName('Person')!.delete()")

        (ctx ! Index.getUserDefinedBySourceUncached(
          auth.scopeID,
          collID)).size shouldBe 0
      }
    }
  }
}
