package fauna.model.test

import fauna.atoms.CollectionID
import fauna.auth.AdminPermissions
import fauna.model.{ Index, Task, TermDataPath }
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.tasks.{ TaskExecutor, TaskRouter }
import fauna.model.Index.IndexByField
import fauna.repo._
import fauna.repo.schema.ConstraintFailure.FQL4UniqueConstraintFailure
import fauna.repo.values.Value
import fauna.repo.Store

class UniqueConstraintSpec extends FQL2WithV4Spec {
  "FQL2 unique constraints" - {
    "unique constraints create unique indexes" in {
      val auth = newAuth
      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    },
            |    {
            |      unique: ["name", "age"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 2
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val termConfigs = idxs.map { idx =>
        ((ctx ! Store.getUnmigrated(auth.scopeID, idx.id.toDocID)).get)
          .data(IndexByField)
      }
      termConfigs.exists { tc =>
        val paths = tc.map(_.path).collect { case TermDataPath(p) => p }
        tc.size == 2 &&
        paths.exists(_ == List(Right("name"))) &&
        paths.exists(_ == List(Right("age")))
      } shouldBe true
      termConfigs.exists { tc =>
        val paths = tc.map(_.path).collect { case TermDataPath(p) => p }
        tc.size == 1 &&
        paths.exists(_ == List(Right("name")))
      } shouldBe true
    }
    "unique constraint indexes are added on update" in {
      val auth = newAuth
      val collID = evalOk(
        auth,
        s"""|Collection.create({
              |  name: "Person",
              |  constraints: [
              |    {
              |      unique: ["name"]
              |    }
              |  ]
              |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]
      evalOk(
        auth,
        s"""|Person.definition.update({
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    },
            |    {
            |     unique: ["name", "age"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 2
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val termConfigs = idxs.map { idx =>
        ((ctx ! Store.getUnmigrated(auth.scopeID, idx.id.toDocID)).get)
          .data(IndexByField)
      }
      termConfigs.exists { tc =>
        val paths = tc.map(_.path).collect { case TermDataPath(p) => p }
        tc.size == 2 &&
        paths.exists(_ == List(Right("name"))) &&
        paths.exists(_ == List(Right("age")))
      } shouldBe true
      termConfigs.exists { tc =>
        val paths = tc.map(_.path).collect { case TermDataPath(p) => p }
        tc.size == 1 &&
        paths.exists(_ == List(Right("name")))
      } shouldBe true
    }
    "unique indexes are deleted when the collection is deleted" in {
      val auth = newAuth
      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    },
            |    {
            |      unique: ["name", "age"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 2

      evalOk(auth, "Person.definition.delete()")
      val idxs2 = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs2 shouldBe empty
    }
    "unique constraint indexes and collection indexes are reused when their definitions match on create" in {
      val auth = newAuth

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |    },
            |  },
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val errRes = evalErr(
        auth,
        s"""|Person.create({
            |  name: "hi",
            |})
            |Person.create({
            |  name: "hi",
            |})
            |""".stripMargin
      )
      errRes.code shouldEqual "constraint_failure"
    }
    "unique constraint indexes re-use existing collection indexes, modifying the existing backing index to enforce uniqueness" in {
      val auth = newAuth

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |    },
            |  },
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]
      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe Unconstrained
      }

      evalOk(
        auth,
        s"""|Person.definition.update({
            |  constraints: [
            |    {
            |      unique: ["name"],
            |    },
            |  ],
            |})""".stripMargin
      )

      val idxs2 = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs2.size shouldBe 1
      idxs2 foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val errRes = evalErr(
        auth,
        s"""|Person.create({
            |  name: "hi",
            |})
            |Person.create({
            |  name: "hi",
            |})
            |""".stripMargin
      )
      errRes.code shouldEqual "constraint_failure"
    }
    "unique constraint backing indexes are re-used by customer defined indexes when the customer index is added via an update" in {
      val auth = newAuth
      mkColl(auth, "Person")
      val collID = evalOk(
        auth,
        s"""|Person.definition.update({
            |  constraints: [
            |    {
            |      unique: ["name"],
            |    },
            |  ],
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      evalOk(
        auth,
        s"""|Person.definition.update({
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |    },
            |  },
            |})""".stripMargin
      )

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val errRes = evalErr(
        auth,
        s"""|Person.create({
            |  name: "hi",
            |})
            |Person.create({
            |  name: "hi",
            |})
            |""".stripMargin
      )
      errRes.code shouldEqual "constraint_failure"
    }
    "deleting a unique constraint that shares a backing index with a user defined index retains the backing index but removes the unique enforcement" in {
      val auth = newAuth

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |    },
            |  },
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      evalOk(
        auth,
        s"""|Person.definition.update({
            |  constraints: []
            |})""".stripMargin
      )
      val idxs2 = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs2.size shouldBe 1
      idxs2 foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe Unconstrained
      }
      evalOk(
        auth,
        s"""|Person.create({
            |  name: "hi",
            |})
            |Person.create({
            |  name: "hi",
            |})
            |""".stripMargin
      )
    }
    "deleting a customer defined index that shares a backing index with a unique constraint should retain the index and the unique enforcement" in {
      val auth = newAuth

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  indexes: {
            |    byNameSortedByName1: {
            |      terms: [{ field: "name" }],
            |    },
            |  },
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1
      idxs foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      evalOk(
        auth,
        s"""|Person.definition.update({
            |  indexes: {
            |    byNameSortedByName1: null,
            |  }
            |})""".stripMargin
      )
      val idxs2 = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs2.size shouldBe 1
      idxs2 foreach { idx =>
        idx.values shouldBe empty
        idx.constraint shouldBe UniqueValues
      }
      val errRes = evalErr(
        auth,
        s"""|Person.create({
            |  name: "hi",
            |})
            |Person.create({
            |  name: "hi",
            |})
            |""".stripMargin
      )
      errRes.code shouldEqual "constraint_failure"
    }

    "cannot cover the same path in two unique constraints" in {
      val auth = newDB

      renderErr(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  constraints: [
            |    {
            |      unique: ["foo", "bar"]
            |    },
            |    {
            |      unique: ["foo", "bar"]
            |    }
            |  ]
            |})""".stripMargin
      ) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  constraints: Duplicate unique constraint on path [.foo, .bar]
           |at *query*:1:18
           |   |
           | 1 |   Collection.create({
           |   |  __________________^
           | 2 | |   name: "Person",
           | 3 | |   constraints: [
           | 4 | |     {
           | 5 | |       unique: ["foo", "bar"]
           | 6 | |     },
           | 7 | |     {
           | 8 | |       unique: ["foo", "bar"]
           | 9 | |     }
           |10 | |   ]
           |11 | | })
           |   | |__^
           |   |""".stripMargin
      )

      renderErr(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    },
            |    {
            |      unique: [".['name']"]
            |    }
            |  ]
            |})""".stripMargin
      ) shouldBe (
        """|error: Failed to create Collection.
           |constraint failures:
           |  constraints: Duplicate unique constraint on path [.name]
           |at *query*:1:18
           |   |
           | 1 |   Collection.create({
           |   |  __________________^
           | 2 | |   name: "Person",
           | 3 | |   constraints: [
           | 4 | |     {
           | 5 | |       unique: ["name"]
           | 6 | |     },
           | 7 | |     {
           | 8 | |       unique: [".['name']"]
           | 9 | |     }
           |10 | |   ]
           |11 | | })
           |   | |__^
           |   |""".stripMargin
      )
    }

    "deleting a unique constraint deletes the backing index" in {
      val auth = newAuth

      val collID = evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Person",
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})""".stripMargin
      ).to[Value.Doc].id.as[CollectionID]

      val idxs = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs.size shouldBe 1

      evalOk(
        auth,
        s"""|Person.definition.update({
            |  constraints: []
            |})""".stripMargin
      )

      val idxs2 = ctx ! Index.getUserDefinedBySourceUncached(
        auth.scopeID,
        collID
      )
      idxs2 shouldBe empty
    }
    "deleting a unique constraint that shares a backing index with a user defined index does not delete the backing index" in {
    }
    "correctly updates unique constraint status to active for a successful async index build" in {
      val auth = newDB

      mkColl(auth, "Person")

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
        s"""|Person.definition.update({
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})
            |""".stripMargin
      )

      evalOk(
        auth,
        "Person.definition.constraints.first()?.status"
      ).as[String] shouldBe "pending"

      // drain runQ
      val executor = TaskExecutor(ctx)
      while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
        executor.step()

      evalOk(
        auth,
        "Person.definition.constraints.first()?.status"
      ).as[String] shouldBe "active"
    }
    "correctly updates unique constraint status to failed for a failed async index build" in {
      val auth = newDB

      mkColl(auth, "Person")

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
        s"""|Person.definition.update({
            |  constraints: [
            |    {
            |      unique: ["name"]
            |    }
            |  ]
            |})
            |""".stripMargin
      )

      evalOk(
        auth,
        "Person.definition.constraints.first()?.status"
      ).as[String] shouldBe "pending"

      // drain runQ
      val tasks = ctx ! Task.getAllRunnable().flattenT
      tasks.size shouldBe 1

      ctx ! TaskRouter.cancel(tasks.head, None)

      evalOk(
        auth,
        "Person.definition.constraints.first()?.status"
      ).as[String] shouldBe "failed"
    }
    "accounts for fql4 unique indexes" in {
      val auth = newDB
      mkColl(auth, "TestColl")
      evalV4Ok(
        auth,
        CreateIndex(
          MkObject(
            "name" -> "TestIndex",
            "source" -> ClassRef("TestColl"),
            "terms" -> List(MkObject("field" -> List("data", "b"))),
            "unique" -> true
          )
        ))

      val errRes = evalErr(
        auth,
        s"""|TestColl.create({
            |   b: "name"
            |})
            |TestColl.create({
            |  b: "name"
            |})
            |""".stripMargin
      )

      errRes.code shouldEqual "constraint_failure"
      errRes.failureMessage shouldEqual "Failed unique constraint."
      val constraintFailures =
        errRes.asInstanceOf[QueryRuntimeFailure].constraintFailures
      constraintFailures.size shouldBe 1
      constraintFailures.head shouldEqual FQL4UniqueConstraintFailure("TestIndex")
    }
    "accounts for internal indexes" in {
      val auth = newDB.withPermissions(AdminPermissions)

      val errRes = evalErr(
        auth,
        s"""|Role.create({
            |   name: "aRole",
            |   privileges: {
            |     resource: "Collection",
            |     actions: {
            |       read: true
            |     }
            |   }
            |})
            |Role.create({
            |  name: "aRole",
            |  privileges: {
            |    resource: "Collection",
            |    actions: {
            |      read: true
            |    }
            |  }
            |})
            |""".stripMargin
      )

      errRes.code shouldEqual "constraint_failure"
      errRes.failureMessage shouldEqual "Failed unique constraint."
      // TODO:: add more validation here once we give better messages for
      // internal index
      // uniqueness failures
    }
  }
}
