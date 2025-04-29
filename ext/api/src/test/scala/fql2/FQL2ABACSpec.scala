package fauna.api.test

import fauna.codex.json._
import fauna.prop.api.{ Database, Query27Helpers }
import fauna.prop.Prop
import org.scalactic.source.Position

class FQL2ABACSpec extends FQL2APISpec with Query27Helpers {
  "FQL2ABACSpec" - {
    // keys

    testX("keys") { (db, privileges) =>
      for {
        role <- aRole(db, Seq.empty, privileges)
      } yield {
        val key = queryOk(s"""Key.create({role: "$role"})""", db)

        (key / "secret").as[String]
      }
    }

    // tokens
    "tokens - identity" - {
      once("roles are able to use authenticated resource's identity") {
        for {
          db      <- aDatabase
          users   <- aCollection(db)
          userA   <- aUserOfCollection(db, users)
          userB   <- aUserOfCollection(db, users)
          profile <- aCollection(db)
          profileA <- aDocument(
            db,
            profile,
            Prop.const(s"""{ user: $users.byId('${userA.id}') }"""))
          profileB <- aDocument(
            db,
            profile,
            Prop.const(s"""{ user: $users.byId('${userB.id}') }"""))

          _ <- aRole(
            db,
            Seq(Membership(users)),
            Seq(
              Privilege(
                profile,
                read = RoleAction("ref => ref.user == Query.identity()"))))
        } {
          assertDenied(s"""$profile.byId('$profileB')!""", userA.token)
          assertAllowed(s"""$profile.byId('$profileA')!""", userA.token)
        }
      }
    }

    "tokens - membership" - {
      once("apply membership predicates") {
        for {
          db         <- aDatabase
          users      <- aCollection(db)
          admin      <- aUserOfCollection(db, users)
          normalUser <- aUserOfCollection(db, users)
          _ <- aRole(
            db,
            Seq(Membership(users, "ref => ref.admin")),
            Seq(Privilege.open(users)))
        } {
          queryOk(s"""$users.byId('${admin.id}')!.update({admin: true})""", db)
          queryOk(s"""$users.byId('${normalUser.id}')!.update({admin: false})""", db)

          assertDenied(s"""$users.create({})""", normalUser.token)
          assertAllowed(s"""$users.create({})""", admin.token)
        }
      }

      once("can use auth functions in membership predicates") {
        for {
          db    <- aDatabase
          cls   <- aCollection(db)
          inst  <- aDocument(db, cls)
          users <- aCollection(db)
          userA <- aUserOfCollection(db, users)
          userB <- aUserOfCollection(db, users)
          _ <- aRole(
            db,
            Seq(
              Membership(
                users,
                s"""ref => $users.byId('${userA.id}') == Query.identity()""")),
            Seq(Privilege.open(cls)))
        } {
          assertDenied(s"""$cls.byId('$inst')!""", userB.token)
          assertAllowed(s"""$cls.byId('$inst')!""", userA.token)
        }
      }
    }

    "tokens - permissions" - {
      once("defaults no access to logged in user") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          user <- aUserOfCollection(db, coll)
        } yield {
          // admin key can update user
          assertAllowed(s"$coll.byId('${user.id}')!.update({})", db.adminKey)

          // token
          assertDenied(s"$coll.byId('${user.id}')!", user.token)
          assertDenied("Query.identity()!", user.token)
          assertDenied(s"$coll.byId('${user.id}')!.update({})", user.token)

          // scoped auth
          val scoped = s"${db.adminKey}:@doc/$coll/${user.id}"
          assertDenied(s"$coll.byId('${user.id}')!", scoped)
          assertDenied("Query.identity()!", scoped)
          assertDenied(s"$coll.byId('${user.id}')!.update({})", scoped)
        }
      }

      once("default no access to logged in user") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          user <- aUserOfCollection(db, coll)
          _    <- aRole(db, Seq(Membership(coll)), Seq(Privilege(coll)))
        } yield {
          // admin key can update user
          assertAllowed(s"$coll.byId('${user.id}')!.update({})", db.adminKey)

          // token
          assertDenied(s"$coll.byId('${user.id}')!", user.token)
          assertDenied("Query.identity()!", user.token)
          assertDenied(s"$coll.byId('${user.id}')!.update({})", user.token)
          assertDenied("Query.identity()!.update({})", user.token)

          // scoped auth
          val scoped = s"${db.adminKey}:@doc/$coll/${user.id}"
          assertDenied(s"$coll.byId('${user.id}')!", scoped)
          assertDenied("Query.identity()!", scoped)
          assertDenied("Query.identity()!.update({})", scoped)
        }
      }

      once("does not allow change logged in user") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          user <- aUserOfCollection(db, coll)
          _ <- aRole(
            db,
            Seq(Membership(coll)),
            Seq(Privilege(coll, read = RoleAction.Granted)))
        } yield {
          // admin key can update user
          assertAllowed(s"""$coll.byId('${user.id}')!.update({})""", db.adminKey)

          // token
          assertAllowed(s"""$coll.byId('${user.id}')!""", user.token)
          assertAllowed(s"""Query.identity()!""", user.token)
          assertDenied(s"""$coll.byId('${user.id}')!.update({})""", user.token)
          assertDenied(s"""$coll.byId('${user.id}')!.delete()""", user.token)

          // document auth
          val token = s"${db.adminKey}:@doc/$coll/${user.id}"
          assertAllowed(s"""$coll.byId('${user.id}')!""", token)
          assertDenied(s"""$coll.byId('${user.id}')!.update({})""", token)
          assertDenied(s"""$coll.byId('${user.id}')!.delete()""", token)
        }
      }

      once("allow change logged in user") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          user1 <- aUserOfCollection(db, coll)
          user2 <- aUserOfCollection(db, coll)
          _     <- aRole(db, Seq(Membership(coll)), Seq(Privilege.open(coll)))
        } yield {
          // token
          assertAllowed(s"""$coll.byId('${user1.id}')!""", user1.token)
          assertAllowed(s"""$coll.byId('${user1.id}')!.update({})""", user1.token)
          assertAllowed(s"""$coll.byId('${user1.id}')!.delete()""", user1.token)

          // document auth
          val token = s"${db.adminKey}:@doc/$coll/${user2.id}"
          assertAllowed(s"""$coll.byId('${user2.id}')!""", token)
          assertAllowed(s"""$coll.byId('${user2.id}')!.update({})""", token)
          assertAllowed(s"""$coll.byId('${user2.id}')!.delete()""", token)
        }
      }

      once("can read own token") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          user <- aUserOfCollection(db, coll)
        } yield {
          // token
          assertAllowed(s"""Query.token()!""", user.token)
        }
      }

      once("cannot edit own token") {
        for {
          db    <- aDatabase
          coll  <- aCollection(db)
          user  <- aUserOfCollection(db, coll)
          user2 <- aUserOfCollection(db, coll)
        } {
          assertDenied(
            s"Query.token()!.update({ document: $coll.byId('${user2.id}') })",
            user.token)
        }
      }

      once("can delete own token") {
        for {
          db   <- aDatabase
          coll <- aCollection(db)
          user <- aUserOfCollection(db, coll)
        } yield {
          assertAllowed(s"Query.token()!.delete()", user.token)
        }
      }
    }

    "indexes" - {
      test(
        "pagination with a role without collection read permission fails with permission denied") {
        val db = aDatabase.sample
        val roleSecret = queryOk(
          """
            |Collection.create({
            |  name: "Books",
            |  indexes: {
            |    byTitle: { terms: [ { field: "title" } ] }
            |  }
            |})
            |
            |Role.create({
            |  name: "TestRole",
            |  privileges: {
            |    resource: "Books",
            |    actions: {}
            |  }
            |})
            |
            |Key.create({ role: "TestRole" }).secret
            |""".stripMargin,
          db
        ).as[String]

        queryOk(
          """
            |Books.create({ title: "t1" })
            |Books.create({ title: "t1" })
            |Books.create({ title: "t1" })
            |""".stripMargin,
          db
        )

        val res = queryOk(
          """Books.byTitle("t1").paginate(1)""".stripMargin,
          db
        )
        val cursor = (res / "after").as[String]

        val errRes = queryErr(
          s"""Set.paginate("$cursor")""".stripMargin,
          roleSecret
        )
        (errRes / "error" / "code").as[String] shouldBe "permission_denied"
        (errRes / "error" / "message").as[
          String] shouldBe "Insufficient privileges to read from collection Books."
      }
    }

    testX("tokens") { (db, privileges) =>
      for {
        users <- aCollection(db)
        user  <- aUserOfCollection(db, users)
        _     <- aRole(db, Seq(Membership(users)), privileges)
      } yield {
        user.token
      }
    }

    test("instance permissions") {
      val db = aDatabase.sample

      queryOk("""Collection.create({name: "Users"})""", db)
      queryOk("""Collection.create({name: "Books"})""", db)

      queryOk(
        """|Role.create({
           |  name: "AdminOnly",
           |  membership: {
           |    resource: "Users",
           |    predicate: 'user => user.isAdmin ?? false'
           |  },
           |  privileges: {
           |    resource: "Books",
           |    actions: {
           |      read: true,
           |      write: true,
           |      create: true,
           |      delete: true
           |    }
           |  }
           |})""".stripMargin,
        db
      )

      queryOk("""Users.create({isAdmin: true})""", db)
      queryOk("""Users.create({isAdmin: false})""", db)
    }

    def testX(subject: String)(
      mkSecret: (Database, Seq[Privilege]) => Prop[String]): Unit = {

      s"$subject - instances" - {
        once("validate read instance permissions with no predicates") {
          for {
            db     <- aDatabase
            classA <- aCollection(db)
            classB <- aCollection(db)
            instA  <- aDocument(db, classA)
            instB  <- aDocument(db, classB)
            secret <- mkSecret(db, Seq(Privilege.open(classA)))
          } {
            assertDenied(s"""$classB.byId('$instB')!""", secret)
            assertAllowed(s"""$classA.byId('$instA')!""", secret)
          }
        }

        once("works when a policy points to a non existent resource") {
          for {
            db     <- aDatabase
            classA <- aCollection(db)
            classB <- aCollection(db)
            classC <- aCollection(db)
            instA  <- aDocument(db, classA)
            _      <- aDocument(db, classB)
            instC  <- aDocument(db, classC)
            secret <- mkSecret(
              db,
              Seq(
                Privilege.open(classA),
                Privilege(
                  classB,
                  read = RoleAction.Granted
                ),
                Privilege(
                  classC,
                  read = RoleAction.Granted
                )
              ))
          } {
            legacyQuery(
              DeleteF(
                Ref(s"collections/$classB")
              ),
              db
            )

            assertAllowed(s"""$classC.byId('$instC')!""", secret)
            assertDenied(s"""$classC.byId('$instC')!.update({foo: 'bar'})""", secret)
            assertDenied(s"""$classC.create({})""", secret)
            assertDenied(s"""$classC.byId('$instC')!.delete()""", secret)

            assertAllowed(
              s"""$classA.byId('$instA')!.update({foo: 'bar'})""",
              secret)
            assertAllowed(s"""$classA.byId('$instA')""", secret)
            assertAllowed(
              s"""$classA.byId('$instA')!.update({foo: 'bar'})""",
              secret)
            assertAllowed(s"""$classA.create({})""", secret)
            assertAllowed(s"""$classA.byId('$instA')!.delete()""", secret)
          }
        }

        once("validate write instance permissions with no predicates") {
          for {
            db     <- aDatabase
            classA <- aCollection(db)
            classB <- aCollection(db)
            instA  <- aDocument(db, classA)
            instB  <- aDocument(db, classB)
            secret <- mkSecret(
              db,
              Seq(
                Privilege.open(classA),
                Privilege(
                  classB,
                  read = RoleAction.Granted
                )
              ))
          } {
            assertAllowed(s"""$classB.byId('$instB')!""", secret)
            assertDenied(s"""$classB.byId('$instB')!.update({foo: 'bar'})""", secret)
            assertDenied(s"""$classB.create({})""", secret)
            assertDenied(s"""$classB.byId('$instB')!.delete()""", secret)

            assertAllowed(
              s"""$classA.byId('$instA')!.update({foo: 'bar'})""",
              secret)
            assertAllowed(s"""$classA.byId('$instA')""", secret)
            assertAllowed(
              s"""$classA.byId('$instA')!.update({foo: 'bar'})""",
              secret)
            assertAllowed(s"""$classA.create({})""", secret)
            assertAllowed(s"""$classA.byId('$instA')!.delete()""", secret)
          }
        }

        once("allow write predicates to have access to the ref being written") {
          for {
            db    <- aDatabase
            cls   <- aCollection(db)
            inst1 <- aDocument(db, cls)
            inst2 <- aDocument(db, cls)
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  cls,
                  write = RoleAction(s"""(old, new) => new.id == '$inst1'"""),
                  read = RoleAction.Granted
                )
              ))
          } {
            assertAllowed(s"""$cls.byId('$inst1')!.update({})""", secret)
            assertDenied(s"""$cls.byId('$inst2')!.update({})""", secret)
          }
        }

        once("validate instance permissions with predicates") {
          for {
            db            <- aDatabase
            cls           <- aCollection(db)
            mutableInst   <- aDocument(db, cls, Prop.const("""{ mutable: true }"""))
            immutableInst <- aDocument(db, cls, Prop.const("""{ mutable: false }"""))
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  cls,
                  write = RoleAction("(old, new) => old.mutable == new.mutable"),
                  read = RoleAction("ref => ref.mutable"),
                  create = RoleAction("data => data.mutable"),
                  delete = RoleAction("ref => ref.mutable")
                ))
            )
          } {
            val immutableData = JSObject("mutable" -> false)
            val mutableData = JSObject("mutable" -> true)
            val anyData = JSObject("foo" -> "bar")

            assertDenied(s"""$cls.byId('$immutableInst')!""", secret)
            assertDenied(
              s"""$cls.byId('$mutableInst')!.update($immutableData)""",
              secret)
            assertDenied(
              s"""$cls.byId('$immutableInst')!.update($mutableData)""",
              secret)
            assertDenied(s"""$cls.create($immutableData)""", secret)
            assertDenied(s"""$cls.byId('$immutableInst')!.delete()""", secret)

            assertAllowed(s"""$cls.byId('$mutableInst')!""", secret)
            assertAllowed(s"""$cls.byId('$mutableInst')!.update($anyData)""", secret)
            assertAllowed(s"""$cls.create($mutableData)""", secret)
            assertAllowed(s"""$cls.byId('$mutableInst')!.delete()""", secret)
          }
        }
      }

      s"$subject - functions" - {
        once("validate function permissions with no predicate") {
          for {
            db     <- aDatabase
            funcA  <- aFunc(db)
            funcB  <- aFunc(db)
            secret <- mkSecret(db, Seq(Privilege.open(funcA)))
          } {
            assertDenied(s"""$funcB("foo")""", secret)
            assertAllowed(s"""$funcA("foo")""", secret)
          }
        }

        once("validate function permissions with predicate") {
          for {
            db   <- aDatabase
            func <- aFunc(db)
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  func,
                  call = RoleAction("""args => args == ["foo", "bar"]""")
                ))
            )
          } {
            assertDenied(s"""$func("baz")""", secret)
            assertAllowed(s"""$func(["foo", "bar"])""", secret)
          }
        }

        once("predicates with read-only user defined functions works") {
          for {
            db        <- aDatabase
            cls       <- aCollection(db)
            cls2      <- aCollection(db)
            inst      <- aDocument(db, cls)
            pureFunc  <- aFunc(db, "() => true")
            writeFunc <- aFunc(db, s"() => { $cls2.create({}); true }")
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  cls,
                  read = RoleAction(s"""ref => $pureFunc()"""),
                  create = RoleAction(s"""data => $writeFunc()""")
                ))
            )
          } {
            assertAllowed(s"""$cls.byId('$inst')!""", secret)
            assertDenied(s"""$cls.create({})""", secret)
          }
        }

        once(
          "user defined functions with roles are restricted to read-only effect") {
          for {
            db       <- aDatabase
            cls      <- aCollection(db)
            cls2     <- aCollection(db)
            inst     <- aDocument(db, cls)
            pureFunc <- aFunc(db, "() => true", role = "\"admin\"")
            writeFunc <- aFunc(
              db,
              s"() => { $cls2.create({}); true }",
              role = "\"admin\"")
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  cls,
                  read = RoleAction(s"ref => $pureFunc()"),
                  create = RoleAction(s"data => $writeFunc()")
                ))
            )
          } {
            assertAllowed(s"""$cls.byId('$inst')!""", secret)
            assertDenied(s"""$cls.create({})""", secret)
          }
        }
      }

      s"$subject - schema" - {
        once("validate schema permissions with no predicate") {
          for {
            db      <- aDatabase
            cls     <- aCollection(db)
            secretA <- mkSecret(db, Seq(Privilege.open("Collection")))
            secretB <- mkSecret(db, Seq(Privilege("Collection")))
          } {
            assertDenied(s"""$cls.definition!""", secretB)
            assertDenied(
              s"""Collection.all().first()!""",
              secretB,
              filteredSet = true)
            assertDenied(s"""Collection.byName('$cls')!""", secretB)
            assertDenied(s"""Collection.create({name: "Foo"})""", secretB)
            assertDenied(s"""$cls.definition.update({name: 'Bar'})""", secretB)
            assertDenied(s"""$cls.definition.delete()""", secretB)

            assertAllowed(s"""$cls.definition!""", secretA)
            assertAllowed(s"""Collection.all().first()!""", secretA)
            assertAllowed(s"""Collection.byName('$cls')!""", secretA)
            assertAllowed(s"""Collection.create({name: "Foo"})""", secretA)
            assertAllowed(s"""$cls.definition.update({name: 'Bar'})""", secretA)
            assertAllowed("""Bar.definition.delete()""", secretA)
          }
        }

        once("validate schema permissions with predicates") {
          for {
            db  <- aDatabase
            cls <- aCollection(db)
            secret <- mkSecret(
              db,
              Seq(
                Privilege(
                  "Collection",
                  create = RoleAction("""config => config.name == "Foo" """),
                  delete = RoleAction("""ref => ref.name == "Foo" """),
                  read = RoleAction("""ref => ref.name == "Foo" """),
                  write = RoleAction("""(old, new) => old.name == "Foo" """)
                ))
            )
          } {
            assertDenied(s"$cls.definition!", secret)
            assertDenied(s"""Collection.create({name: "Bar"})""", secret)
            assertDenied(s"""$cls.definition.update({name: "Baz"})""", secret)
            assertDenied(s"""$cls.definition.delete()""", secret)

            assertAllowed(s"""Collection.create({name: "Foo"})""", secret)
            assertAllowed(s"""Foo.definition!""", secret)
            assertAllowed(s"""Foo.definition.update({})""", secret)
            assertAllowed(s"""Foo.definition.delete()""", secret)
          }
        }
      }

      s"$subject - temporality" - {
        once("requires history_read permission to read docs with `at(..)`") {
          for {
            db  <- aDatabase
            col <- aHistoryCollection(db)
            priv = Privilege(col)
            secretA <- mkSecret(db, Seq(priv.copy(historyRead = RoleAction.Granted)))
            secretB <- mkSecret(db, Seq(priv.copy(historyRead = RoleAction.Denied)))
            query = s"at(Time.epoch(0, 'seconds')) { $col.byId('1')! }"
          } {
            // insert doc via v4 since historical writes are not supported in v10
            legacyQuery(InsertVers(MkRef(ClassRef(col), "1"), 0, "create"), db)
            assertAllowed(query, secretA)
            assertDenied(query, secretB)
          }
        }

        once("requires history_read permission to read sets with `at(..)`") {
          for {
            db  <- aDatabase
            col <- aHistoryCollection(db)
            priv = Privilege(col)
            secretA <- mkSecret(db, Seq(priv.copy(historyRead = RoleAction.Granted)))
            secretB <- mkSecret(db, Seq(priv.copy(historyRead = RoleAction.Denied)))
            query = s"at(Time.epoch(0, 'seconds')) { $col.all().paginate(32)! }"
          } {
            // insert doc via v4 since historical writes are not supported in v10
            for (i <- 1 to 64) {
              legacyQuery(InsertVers(MkRef(ClassRef(col), i), 0, "create"), db)
            }

            val res = assertAllowed(query, secretA)
            val data = (res / "data").as[Seq[JSValue]]
            val cursor = (res / "after").as[String]
            data should not be empty

            val res0 = assertAllowed(s"Set.paginate('$cursor')", secretA)
            val data0 = (res0 / "data").as[Seq[JSValue]]
            data0 should not be empty

            // NB. Sets reads are filtered instead o rejected.
            val res1 = assertAllowed(query, secretB)
            val data1 = (res1 / "data").as[Seq[JSValue]]
            data1 shouldBe empty

            val res2 = assertAllowed(s"Set.paginate('$cursor')", secretB)
            val data2 = (res2 / "data").as[Seq[JSValue]]
            data2 shouldBe empty
          }
        }

        once("do NOT require history_read when paginating non-temporal cursors") {
          for {
            db     <- aDatabase
            col    <- aCollection(db)
            _      <- aDocument(db, col, data = Prop.const("{ foo: 42 }")) * 64
            secret <- mkSecret(db, Seq(Privilege(col, read = RoleAction.Granted)))
          } {
            val res = assertAllowed(s"$col.all().map(.foo).paginate(32)!", secret)
            val data = (res / "data").as[Seq[JSValue]]
            val cursor = (res / "after").as[String]
            data should not be empty

            val res0 = assertAllowed(s"Set.paginate('$cursor')", secret)
            val data0 = (res0 / "data").as[Seq[JSValue]]
            val cursor0 = (res0 / "after").asOpt[String]
            data0 should not be empty
            cursor0 shouldBe empty
          }
        }
      }
    }
  }

  def assertAllowed(query: String, token: String)(implicit pos: Position) = {
    queryOk(query, token)
  }

  def assertDenied(query: String, token: String, filteredSet: Boolean = false)(
    implicit pos: Position) = {
    val res = queryErr(query, token)

    (res / "error" / "code").as[String] match {
      case "permission_denied" =>
        (res / "error" / "message")
          .as[String] shouldBe "Insufficient privileges to perform the action."

      case "null_value" if filteredSet =>
        (res / "error" / "message")
          .as[String] shouldBe "Null value, due to empty set"

      case "null_value" =>
        (res / "error" / "message")
          .as[String] shouldBe "Null value, due to permission denied"

      case _ =>
        fail(s"Unexpected error ${(res / "summary").as[String]}")
    }
  }
}
