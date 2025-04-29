package fauna.model.test

import fauna.ast.VersionL
import fauna.auth.{ AdminPermissions, RootAuth }
import fauna.lang.clocks.Clock
import fauna.model.test.SocialHelpers.{
  evalQuery,
  mkDoc,
  runQuery,
  CreateDatabase,
  DatabaseRef,
  MkObject,
  RoleRef,
  Update
}
import fauna.prop.api.Query27Helpers.{
  ClassRef,
  CreateCollection,
  CreateF,
  CreateFunction,
  CreateKey,
  CreateRole,
  DeleteF,
  FunctionRef,
  KeysRef,
  Lambda,
  MkRef,
  QueryF,
  Var
}
import fauna.repo.test.CassandraHelper
import fauna.stats.{ QueryMetrics, StatsRequestBuffer }

class QueryMetricsSpec extends FQL2Spec {
  "documents" - {
    "v4" - {
      "increments creates, does not count native collection" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            ))
        )
        val docQ = evalQuery(
          RootAuth,
          CreateF(coll1, MkObject("data" -> MkObject("foo" -> "bar"))))
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              CreateF(coll1, MkObject("data" -> MkObject("foo" -> "bar")))
            )
          }
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateCollection(
                MkObject(
                  "name" -> coll2
                ))
            ).flatMap { _ =>
              evalQuery(
                RootAuth,
                CreateDatabase(MkObject(
                  "name" -> "testdb"
                )))

            }.flatMap { _ =>
              evalQuery(
                RootAuth,
                CreateRole(MkObject(
                  "name" -> "testRole"
                )))

            }.flatMap { _ =>
              evalQuery(
                RootAuth,
                CreateFunction(
                  MkObject(
                    "name" -> "foofunc",
                    "body" -> QueryF(Lambda("x" -> Var("x")))))
              )
            }.flatMap { _ =>
              evalQuery(
                RootAuth,
                CreateKey(MkObject("role" -> "server"))
              )
            }
          }
        CassandraHelper.withStats(stats) { ctx ! docQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.documentCreates shouldEqual 2
      }
      "increments updates, does not count native collection" in {
        val db1 = aUniqueIdentifier.sample
        val db2 = aUniqueIdentifier.sample
        val coll1 = aUniqueIdentifier.sample
        val func1 = aUniqueIdentifier.sample
        val role1 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            ))
        )
        val doc = ctx ! mkDoc(
          RootAuth,
          coll1,
          params = MkObject("data" -> MkObject("foo" -> "bar"))
        )
        val doc2 = ctx ! mkDoc(
          RootAuth,
          coll1,
          params = MkObject("data" -> MkObject("foo" -> "bar"))
        )

        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateDatabase(
            MkObject(
              "name" -> db1
            )))

        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateRole(
            MkObject(
              "name" -> role1
            )))

        ctx ! evalQuery(
          RootAuth,
          CreateFunction(
            MkObject("name" -> func1, "body" -> QueryF(Lambda("x" -> Var("x")))))
        )

        val key = ctx ! runQuery(
          RootAuth,
          CreateKey(MkObject("role" -> "server"))
        )

        val keyRef = key match {
          case kv: VersionL =>
            MkRef(KeysRef, kv.version.docID.subID.toLong)
          case v =>
            fail(
              s"unexpected query eval time creating key, expected VersionL, got $v")
        }

        val docQ = runQuery(
          RootAuth,
          Update(doc.refObj, MkObject("data" -> MkObject("bar" -> "baz"))))
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              Update(doc2.refObj, MkObject("data" -> MkObject("bar" -> "baz"))))
          }
          .flatMap { _ =>
            runQuery(
              RootAuth,
              Clock.time,
              Update(
                ClassRef(coll1),
                MkObject(
                  "history_days" -> 1
                ))
            ).flatMap { _ =>
              runQuery(
                RootAuth,
                Update(
                  DatabaseRef(db1),
                  MkObject(
                    "name" -> db2
                  )))
            }.flatMap { _ =>
              runQuery(
                RootAuth,
                Update(
                  RoleRef(role1),
                  MkObject(
                    "name" -> "new_role_name"
                  )))
            }.flatMap { _ =>
              runQuery(
                RootAuth,
                Update(
                  FunctionRef(func1),
                  MkObject(
                    "name" -> "new_func_name"
                  )))
            }.flatMap { _ =>
              runQuery(
                RootAuth,
                Update(
                  keyRef,
                  MkObject(
                    "role" -> "admin"
                  )))
            }
          }
        CassandraHelper.withStats(stats) { ctx ! docQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )

        queryMetrics.documentUpdates shouldEqual 2
      }
      "increments deletes, does not count native collection" in {
        val db1 = aUniqueIdentifier.sample
        val role1 = aUniqueIdentifier.sample
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val func1 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! runQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            ))
        )
        ctx ! runQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll2
            ))
        )
        ctx ! runQuery(
          RootAuth,
          Clock.time,
          CreateRole(
            MkObject(
              "name" -> role1
            ))
        )
        val key = ctx ! runQuery(
          RootAuth,
          CreateKey(MkObject("role" -> "server"))
        )
        val keyRef = key match {
          case kv: VersionL =>
            MkRef(KeysRef, kv.version.docID.subID.toLong)
          case v =>
            fail(
              s"unexpected query eval time creating key, expected VersionL, got $v")
        }

        ctx ! runQuery(
          RootAuth,
          CreateFunction(
            MkObject("name" -> func1, "body" -> QueryF(Lambda("x" -> Var("x")))))
        )

        val doc = ctx ! mkDoc(
          RootAuth,
          coll1,
          params = MkObject("data" -> MkObject("foo" -> "bar"))
        )
        val doc2 = ctx ! mkDoc(
          RootAuth,
          coll1,
          params = MkObject("data" -> MkObject("foo" -> "bar"))
        )

        ctx ! runQuery(
          RootAuth,
          Clock.time,
          CreateDatabase(
            MkObject(
              "name" -> db1
            )))

        val docQ = for {
          // The database update creates schema contention and retries, so run it
          // first (to make our counts not include those retries).
          _ <- runQuery(RootAuth, DeleteF(DatabaseRef(db1)))
          _ <- runQuery(RootAuth, DeleteF(ClassRef(coll2)))
          _ <- runQuery(RootAuth, DeleteF(RoleRef(role1)))
          _ <- runQuery(RootAuth, DeleteF(FunctionRef(func1)))
          _ <- runQuery(RootAuth, DeleteF(keyRef))
          _ <- runQuery(RootAuth, DeleteF(doc.refObj))
          _ <- runQuery(RootAuth, DeleteF(doc2.refObj))
        } yield ()
        CassandraHelper.withStats(stats) { ctx ! docQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.documentDeletes shouldEqual 2
      }
    }
    "v10" - {
      "increments creates, doesn't count native collections" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val auth = newDB

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Collection.create({ name: "$coll1" })
              |""".stripMargin
        )

        // todo test for other native collections once we are counting those
        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.create({ name: "$coll2" })
                |Database.create({ name: "testdb" })
                |Role.create({ name: "testrole" })
                |Function.create({ name: "testFunc", body: '(x) => x' })
                |Key.create({ role: "admin" })
                |$coll1.create({})
                |$coll1.create({})
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.documentCreates shouldEqual 2
      }

      "increments updates, doesn't count native collections" in {
        val coll1 = aUniqueIdentifier.sample
        val db1 = aUniqueIdentifier.sample
        val role1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val func1 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val auth = newDB

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Collection.create({ name: "$coll1" })
              |Collection.create({ name: "$coll2" })
              |Database.create({ name: "$db1" })
              |Function.create({ name: "$func1", body: '(x) => x' })
              |Key.create({ role: "admin" })
              |Role.create({ name: "$role1" })
              |""".stripMargin
        )

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |$coll1.create({})
              |$coll1.create({})
              |""".stripMargin
        )

        // todo test for other native collections once we are counting those
        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.byName("$coll2")!.update({ history_days: 3 })
                |Database.byName("$db1")!.update({ name: "testdb" })
                |Role.byName("$role1")!.update({ name: "testrole" })
                |Function.byName("$func1")!.update({ name: "func2" })
                |Key.all().first()!.update({ role: "server" })
                |$coll1.all().forEach(.update({ test: "update" }))
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.documentUpdates shouldEqual 2
      }
      "increments deletes, doesn't count native collections" in {
        val coll1 = aUniqueIdentifier.sample
        val func1 = aUniqueIdentifier.sample
        val db1 = aUniqueIdentifier.sample
        val role1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val auth = newDB

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Collection.create({ name: "$coll1" })
              |Collection.create({ name: "$coll2" })
              |Function.create({ name: "$func1", body: '(x) => x' })
              |Database.create({ name: "$db1" })
              |Key.create({ role: "admin" })
              |Role.create({ name: "$role1" })
              |""".stripMargin
        )

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |$coll1.create({})
              |$coll1.create({})
              |""".stripMargin
        )

        // todo test for other native collections once we are counting those
        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.byName("$coll2")!.delete()
                |Database.byName("$db1")!.delete()
                |Role.byName("$role1")!.delete()
                |Function.byName("$func1")!.delete()
                |Key.all().first()!.delete()
                |$coll1.all().forEach(.delete())
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.documentDeletes shouldEqual 2
      }

    }
  }
  "keys" - {
    "v4" - {
      "increments creates" in {
        val stats = new StatsRequestBuffer
        val dbQ = evalQuery(
          RootAuth,
          Clock.time,
          CreateKey(
            MkObject(
              "role" -> "admin"
            ))).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateKey(
              MkObject(
                "role" -> "admin"
              ))
          ).flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateKey(
                MkObject(
                  "role" -> "admin"
                ))
            )
          }
        }
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyCreates shouldEqual 3
      }

      "increments updates" in {
        val stats = new StatsRequestBuffer
        val key = ctx ! runQuery(
          RootAuth,
          CreateKey(MkObject("role" -> "admin"))
        )

        val keyRef = key match {
          case kv: VersionL =>
            MkRef(KeysRef, kv.version.docID.subID.toLong)
          case v =>
            fail(
              s"unexpected query eval time creating key, expected VersionL, got $v")
        }
        val dbQ = runQuery(
          RootAuth,
          Clock.time,
          Update(keyRef, MkObject("role" -> "server"))
        )
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyUpdates shouldEqual 1
      }

      "increments deletes" in {
        val stats = new StatsRequestBuffer
        val key = ctx ! runQuery(
          RootAuth,
          CreateKey(MkObject("role" -> "admin"))
        )

        val keyRef = key match {
          case kv: VersionL =>
            MkRef(KeysRef, kv.version.docID.subID.toLong)
          case v =>
            fail(
              s"unexpected query eval time creating key, expected VersionL, got $v")
        }
        val dbQ = runQuery(
          RootAuth,
          Clock.time,
          DeleteF(keyRef)
        )
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyDeletes shouldEqual 1
      }
    }
    "v10" - {
      "increments create" in {
        val stats = new StatsRequestBuffer

        val auth = newDB

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Key.create({ role: "admin" })
                |Key.create({ role: "admin" })
                |Key.create({ role: "admin" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyCreates shouldEqual 3
      }
      "increments update" in {
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""Key.create({ role: "admin" })"""
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Key.all().first()!.update({ role: "server" }).update({ role: "server-readonly" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyUpdates shouldEqual 2
      }
      "increments delete" in {
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Key.create({ role: "admin" })
              |Key.create({ role: "admin" })
              |Key.create({ role: "admin" })
              |""".stripMargin
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Key.all().forEach(.delete())
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.keyDeletes shouldEqual 3
      }
    }
  }
  "functions" - {
    "v4" - {
      "increments creates" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val function3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val dbQ = evalQuery(
          RootAuth,
          Clock.time,
          CreateFunction(
            MkObject(
              "name" -> function1,
              "body" -> QueryF(Lambda("x" -> Var("x")))))).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateFunction(
              MkObject(
                "name" -> function2,
                "body" -> QueryF(Lambda("x" -> Var("x")))))
          ).flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateFunction(
                MkObject(
                  "name" -> function3,
                  "body" -> QueryF(Lambda("x" -> Var("x")))))
            )
          }
        }
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionCreates shouldEqual 3
      }
      "increments updates" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateFunction(
            MkObject("name" -> function1, "body" -> QueryF(Lambda("x" -> Var("x")))))
        )
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateFunction(
            MkObject("name" -> function2, "body" -> QueryF(Lambda("x" -> Var("x")))))
        )

        val collQ = evalQuery(
          RootAuth,
          Clock.time,
          Update(
            FunctionRef(function1),
            MkObject(
              "data" -> MkObject("desc" -> "update1")
            ))
        ).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            Update(
              FunctionRef(function2),
              MkObject(
                "data" -> MkObject("desc" -> "update2")
              ))
          )
        }
        CassandraHelper.withStats(stats) {
          ctx ! collQ
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionUpdates shouldEqual 2
      }
      "increments deletes" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val function3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateFunction(
            MkObject(
              "name" -> function1,
              "body" -> QueryF(Lambda("x" -> Var("x")))
            )))
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateFunction(
                MkObject(
                  "name" -> function2,
                  "body" -> QueryF(Lambda("x" -> Var("x")))
                ))
            ).flatMap { _ =>
              evalQuery(
                RootAuth,
                Clock.time,
                CreateFunction(
                  MkObject(
                    "name" -> function3,
                    "body" -> QueryF(Lambda("x" -> Var("x")))
                  ))
              )
            }
          }

        val delQ =
          evalQuery(
            RootAuth,
            Clock.time,
            DeleteF(FunctionRef(function1))
          )
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(FunctionRef(function2)))
            }
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(FunctionRef(function3)))
            }

        CassandraHelper.withStats(stats) {
          ctx ! delQ
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionDeletes shouldEqual 3
      }
    }
    "v10" - {
      "increments create" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val function3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Function.create({ name: "$function1", body: '(x) => x' })
                |Function.create({ name: "$function2", body: '(x) => x' })
                |Function.create({ name: "$function3", body: '(x) => x' })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionCreates shouldEqual 3
      }
      "increments update" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val function3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""Function.create({ name: "$function1", body: '(x) => x' })"""
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Function.byName("$function1")!.update({ name: "$function2" }).update({ name: "$function3" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionUpdates shouldEqual 2
      }
      "increments delete" in {
        val function1 = aUniqueIdentifier.sample
        val function2 = aUniqueIdentifier.sample
        val function3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Function.create({ name: "$function1", body: '(x) => x' })
              |Function.create({ name: "$function2", body: '(x) => x' })
              |Function.create({ name: "$function3", body: '(x) => x' })
              |""".stripMargin
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Function.byName("$function1")!.delete()
                |Function.byName("$function2")!.delete()
                |Function.byName("$function3")!.delete()
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.functionDeletes shouldEqual 3
      }
    }
  }
  "roles" - {
    "v4" - {
      "increments creates" in {
        val role1 = aUniqueIdentifier.sample
        val role2 = aUniqueIdentifier.sample
        val role3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val dbQ = evalQuery(
          RootAuth,
          Clock.time,
          CreateRole(
            MkObject(
              "name" -> role1
            ))).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateRole(
              MkObject(
                "name" -> role2
              ))
          ).flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateRole(
                MkObject(
                  "name" -> role3
                ))
            )
          }
        }
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleCreates shouldEqual 3
      }
      "increments updates" in {
        val role1 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateRole(
            MkObject(
              "name" -> role1
            ))
        )

        val collQ = evalQuery(
          RootAuth,
          Clock.time,
          Update(
            RoleRef(role1),
            MkObject(
              "data" -> MkObject("desc" -> "updattee")
            ))
        ).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            Update(
              RoleRef(role1),
              MkObject(
                "data" -> MkObject("desc" -> "upgraddee")
              ))
          )
        }
        CassandraHelper.withStats(stats) {
          ctx ! collQ
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleUpdates shouldEqual 2
      }
      "increments deletes" in {
        val role1 = aUniqueIdentifier.sample
        val role2 = aUniqueIdentifier.sample
        val role3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateRole(
            MkObject(
              "name" -> role1
            )))
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateRole(
                MkObject(
                  "name" -> role2
                ))
            ).flatMap { _ =>
              evalQuery(
                RootAuth,
                Clock.time,
                CreateRole(
                  MkObject(
                    "name" -> role3
                  ))
              )
            }
          }

        val delQ =
          evalQuery(
            RootAuth,
            Clock.time,
            DeleteF(RoleRef(role1))
          )
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(RoleRef(role2)))
            }
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(RoleRef(role3)))
            }

        CassandraHelper.withStats(stats) {
          ctx ! delQ
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleDeletes shouldEqual 3
      }
    }
    "v10" - {
      "increments create" in {
        val role1 = aUniqueIdentifier.sample
        val role2 = aUniqueIdentifier.sample
        val role3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Role.create({ name: "$role1" })
                |Role.create({ name: "$role2" })
                |Role.create({ name: "$role3" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleCreates shouldEqual 3
      }
      "increments update" in {
        val role1 = aUniqueIdentifier.sample
        val role2 = aUniqueIdentifier.sample
        val role3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""Role.create({ name: "$role1" })"""
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Role.byName("$role1")!.update({ name: "$role2" }).update({ name: "$role3" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleUpdates shouldEqual 2
      }
      "increments delete" in {
        val role1 = aUniqueIdentifier.sample
        val role2 = aUniqueIdentifier.sample
        val role3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Role.create({ name: "$role1" })
              |Role.create({ name: "$role2" })
              |Role.create({ name: "$role3" })
              |""".stripMargin
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Role.byName("$role1")!.delete()
                |Role.byName("$role2")!.delete()
                |Role.byName("$role3")!.delete()
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleDeletes shouldEqual 3
      }
    }
    "schema" - {
      "increments create" in {
        val auth = newDB.withPermissions(AdminPermissions)
        val stats = new StatsRequestBuffer
        CassandraHelper.withStats(stats) {
          updateSchemaOk(
            auth,
            "main.fsl" ->
              """|role A0 {
                 |}
                 |
                 |role A1 {
                 |}
                 |""".stripMargin
          )
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleCreates shouldEqual 2
      }
      "increments update" in {
        val auth = newDB.withPermissions(AdminPermissions)
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|role A0 {
               |}
               |
               |role A1 {
               |}
               |
               |collection TestColl {}
               |""".stripMargin
        )
        val stats = new StatsRequestBuffer
        eventually {
          CassandraHelper.withStats(stats) {
            updateSchemaOk(
              auth,
              "main.fsl" ->
                """|role A0 {
                   |  privileges TestColl {
                   |    read
                   |  }
                   |}
                   |
                   |role A1 {
                   |  privileges TestColl {
                   |    read
                   |  }
                   |}
                   |
                   |collection TestColl {}
                   |""".stripMargin
            )
          }
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleUpdates shouldEqual 2
      }
      "increments delete" in {
        val auth = newDB.withPermissions(AdminPermissions)
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|role A0 {
               |}
               |
               |role A1 {
               |}
               |""".stripMargin
        )
        val stats = new StatsRequestBuffer
        CassandraHelper.withStats(stats) {
          updateSchemaOk(
            auth,
            "main.fsl" ->
              """|
                 |""".stripMargin
          )
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.roleDeletes shouldEqual 2
      }
    }
  }
  "collection" - {
    "v4" - {
      "increments creates" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val coll3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        val dbQ = evalQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            ))).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateCollection(
              MkObject(
                "name" -> coll2
              ))
          ).flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateCollection(
                MkObject(
                  "name" -> coll3
                ))
            )
          }
        }
        CassandraHelper.withStats(stats) { ctx ! dbQ }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionCreates shouldEqual 3
      }
      "increments updates" in {
        val coll1 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            ))
        )

        val collQ = evalQuery(
          RootAuth,
          Clock.time,
          Update(
            ClassRef(coll1),
            MkObject(
              "history_days" -> 1
            ))
        ).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            Update(
              ClassRef(coll1),
              MkObject(
                "history_days" -> 2
              ))
          )
        }
        CassandraHelper.withStats(stats) {
          ctx ! collQ
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionUpdates shouldEqual 2
      }
      "increments deletes" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val coll3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer
        ctx ! evalQuery(
          RootAuth,
          Clock.time,
          CreateCollection(
            MkObject(
              "name" -> coll1
            )))
          .flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateCollection(
                MkObject(
                  "name" -> coll2
                ))
            ).flatMap { _ =>
              evalQuery(
                RootAuth,
                Clock.time,
                CreateCollection(
                  MkObject(
                    "name" -> coll3
                  ))
              )
            }
          }

        val delQ =
          evalQuery(
            RootAuth,
            Clock.time,
            DeleteF(ClassRef(coll1))
          )
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(ClassRef(coll2)))
            }
            .flatMap { _ =>
              evalQuery(RootAuth, Clock.time, DeleteF(ClassRef(coll3)))
            }

        CassandraHelper.withStats(stats) {
          ctx ! delQ
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionDeletes shouldEqual 3
      }
    }
    "v10" - {
      "increments create" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val coll3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.create({ name: "$coll1" })
                |Collection.create({ name: "$coll2" })
                |Collection.create({ name: "$coll3" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionCreates shouldEqual 3
      }
      "increments update" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val coll3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""Collection.create({ name: "$coll1" })"""
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.byName("$coll1")!.update({ name: "$coll2" }).update({ name: "$coll3" })
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionUpdates shouldEqual 2
      }
      "increments delete" in {
        val coll1 = aUniqueIdentifier.sample
        val coll2 = aUniqueIdentifier.sample
        val coll3 = aUniqueIdentifier.sample
        val stats = new StatsRequestBuffer

        val auth = newDB
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Collection.create({ name: "$coll1" })
              |Collection.create({ name: "$coll2" })
              |Collection.create({ name: "$coll3" })
              |""".stripMargin
        )

        CassandraHelper.withStats(stats) {
          evalOk(
            auth.withPermissions(AdminPermissions),
            s"""|
                |Collection.byName("$coll1")!.delete()
                |Collection.byName("$coll2")!.delete()
                |Collection.byName("$coll3")!.delete()
                |""".stripMargin
          )
        }

        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionDeletes shouldEqual 3
      }
    }
    "schema" - {
      "increments create" in {
        val auth = newDB.withPermissions(AdminPermissions)
        val stats = new StatsRequestBuffer
        CassandraHelper.withStats(stats) {
          updateSchemaOk(
            auth,
            "main.fsl" ->
              """|collection A0 {
                 |  name: String
                 |}
                 |
                 |collection A1 {
                 |  name: String
                 |}
                 |""".stripMargin
          )
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionCreates shouldEqual 2
      }
      "increments update" in {
        val auth = newDB.withPermissions(AdminPermissions)
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|collection A0 {
               |  name: String
               |}
               |
               |collection A1 {
               |  name: String
               |}
               |""".stripMargin
        )
        val stats = new StatsRequestBuffer
        CassandraHelper.withStats(stats) {
          updateSchemaOk(
            auth,
            "main.fsl" ->
              """|collection A0 {
                 |  name: String
                 |  history_days: 1
                 |}
                 |
                 |collection A1 {
                 |  name: String
                 |  history_days: 1
                 |}
                 |""".stripMargin
          )
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionUpdates shouldEqual 2
      }
      "increments delete" in {
        val auth = newDB.withPermissions(AdminPermissions)
        updateSchemaOk(
          auth,
          "main.fsl" ->
            """|collection A0 {
               |  name: String
               |}
               |
               |collection A1 {
               |  name: String
               |}
               |""".stripMargin
        )
        val stats = new StatsRequestBuffer
        CassandraHelper.withStats(stats) {
          updateSchemaOk(
            auth,
            "main.fsl" ->
              """|
                 |""".stripMargin
          )
        }
        val queryMetrics = QueryMetrics(
          queryTime = 1,
          stats = stats
        )
        queryMetrics.collectionDeletes shouldEqual 2
      }
    }

  }

  "v4" - {
    "increments database creates" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer
      val dbQ = evalQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> db1
          ))).flatMap { _ =>
        evalQuery(
          RootAuth,
          Clock.time,
          CreateDatabase(
            MkObject(
              "name" -> db2
            ))
        ).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateDatabase(
              MkObject(
                "name" -> db3
              ))
          )
        }
      }
      CassandraHelper.withStats(stats) { ctx ! dbQ }
      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseCreates shouldEqual 3
    }
    "increments database updates" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer
      val dbQ = evalQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> db1
          ))).flatMap { _ =>
        evalQuery(
          RootAuth,
          Clock.time,
          Update(
            DatabaseRef(db1),
            MkObject(
              "name" -> db2
            ))
        ).flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            Update(
              DatabaseRef(db2),
              MkObject(
                "name" -> db3
              ))
          )
        }
      }
      CassandraHelper.withStats(stats) { ctx ! dbQ }
      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseUpdates shouldEqual 2
    }
    "increments database deletes" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer
      ctx ! evalQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> db1
          )))
        .flatMap { _ =>
          evalQuery(
            RootAuth,
            Clock.time,
            CreateDatabase(
              MkObject(
                "name" -> db2
              ))
          ).flatMap { _ =>
            evalQuery(
              RootAuth,
              Clock.time,
              CreateDatabase(
                MkObject(
                  "name" -> db3
                ))
            )
          }
        }

      val delQ =
        evalQuery(
          RootAuth,
          Clock.time,
          DeleteF(DatabaseRef(db1))
        )
          .flatMap { _ =>
            evalQuery(RootAuth, Clock.time, DeleteF(DatabaseRef(db2)))
          }
          .flatMap { _ =>
            evalQuery(RootAuth, Clock.time, DeleteF(DatabaseRef(db3)))
          }

      CassandraHelper.withStats(stats) {
        ctx ! delQ
      }

      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseDeletes shouldEqual 3
    }
  }
  "v10" - {
    "increments database creates" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer

      val auth = newDB

      CassandraHelper.withStats(stats) {
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Database.create({ name: "$db1" })
              |Database.create({ name: "$db2" })
              |Database.create({ name: "$db3" })
              |""".stripMargin
        )
      }

      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseCreates shouldEqual 3
    }
    "increments database updates" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer

      val auth = newDB
      evalOk(
        auth.withPermissions(AdminPermissions),
        s"""Database.create({ name: "$db1" })"""
      )

      CassandraHelper.withStats(stats) {
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Database.byName("$db1")!.update({ name: "$db2" }).update({ name: "$db3" })
              |""".stripMargin
        )
      }

      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseUpdates shouldEqual 2
    }
    "increments database deletes" in {
      val db1 = aUniqueIdentifier.sample
      val db2 = aUniqueIdentifier.sample
      val db3 = aUniqueIdentifier.sample
      val stats = new StatsRequestBuffer

      val auth = newDB
      evalOk(
        auth.withPermissions(AdminPermissions),
        s"""|
            |Database.create({ name: "$db1" })
            |Database.create({ name: "$db2" })
            |Database.create({ name: "$db3" })
            |""".stripMargin
      )

      CassandraHelper.withStats(stats) {
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|
              |Database.byName("$db1")!.delete()
              |Database.byName("$db2")!.delete()
              |Database.byName("$db3")!.delete()
              |""".stripMargin
        )
      }

      val queryMetrics = QueryMetrics(
        queryTime = 1,
        stats = stats
      )
      queryMetrics.databaseDeletes shouldEqual 3
    }
  }
}
