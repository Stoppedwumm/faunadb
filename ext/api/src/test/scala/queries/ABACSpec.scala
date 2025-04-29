package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.prop._
import fauna.prop.api._

class ABACSpec extends QueryAPI4Spec {

  "tokens" - {
    once("cannot edit themselves") {
      for {
        db   <- aDatabase
        cls  <- aCollection(db)
        doc  <- aDocument(cls)
        doc2 <- aDocument(cls)
      } {
        runQuery(
          CreateF(
            CredentialsRef,
            MkObject("instance" -> doc.refObj, "password" -> "secret")),
          db.adminKey)
        val tok =
          runQuery(Login(doc.refObj, MkObject("password" -> "secret")), db.adminKey)
        val secret = (tok / "secret").as[String]

        qassertErr(
          Update(tok.refObj, MkObject("instance" -> doc2.refObj)),
          "permission denied",
          JSArray(),
          secret)
      }
    }
  }

  "roles" - {
    once("limit the maximum number of roles per resource") {
      for {
        db    <- aDatabase
        cls   <- aCollection(db)
        roles <- aRole(db, Membership(cls), Privilege.open(cls.refObj)) times 64
      } yield {
        val exceedingRole = CreateRole(
          MkObject(
            "name" -> "exceeding_role",
            "membership" -> MkObject(
              "resource" -> cls.refObj
            ),
            "privileges" -> MkObject(
              "resource" -> cls.refObj,
              "actions" -> MkObject(
                "read" -> true
              )
            )
          ))

        qassertErr(
          exceedingRole,
          "validation failed",
          JSArray("create_role"),
          db.adminKey)
        runQuery(DeleteF(roles.head.refObj), db.adminKey)
        runQuery(exceedingRole, db.adminKey)
      }
    }

    once("allow omit actions") {
      for {
        db  <- aDatabase
        cls <- aCollection(db)
      } yield {
        runQuery(
          CreateRole(
            MkObject(
              "name" -> "aRole",
              "privileges" -> MkObject("resource" -> cls.refObj)
            )),
          db.adminKey
        )

        val key = runQuery(
          CreateKey(MkObject("role" -> RoleRef("aRole"))),
          db.adminKey
        )

        runQuery("works", (key / "secret").as[String]).as[String] shouldBe "works"
      }
    }

    once("allow omit privileges") {
      for {
        db <- aDatabase
      } yield {
        runQuery(
          CreateRole(MkObject("name" -> "aRole")),
          db.adminKey
        )

        val key = runQuery(
          CreateKey(MkObject("role" -> RoleRef("aRole"))),
          db.adminKey
        )

        runQuery("works", (key / "secret").as[String]).as[String] shouldBe "works"
      }
    }
  }

  test("access keys") { (db, privileges) =>
    for {
      role <- aRole(db, privileges: _*)
      key  <- aKey(role)
    } yield {
      key.secret
    }
  }

  test("tokens") { (db, privileges) =>
    for {
      user <- aUser(db)
      _    <- aRole(db, Membership(user.collection), privileges: _*)
    } yield {
      user.token
    }
  }

  "tokens - identity" - {
    once("roles are able to use authenticated resource's identity") {
      for {
        db      <- aDatabase
        users   <- aCollection(db)
        userA   <- aUserOfCollection(db, users)
        userB   <- aUserOfCollection(db, users)
        profile <- aCollection(db)
        profileA <- aDocument(
          profile,
          dataProp = jsObject("user" -> Prop.const(userA.refObj)))
        profileB <- aDocument(
          profile,
          dataProp = jsObject("user" -> Prop.const(userB.refObj)))
        _ <- aRole(
          db,
          Membership(users),
          Privilege(
            profile.refObj,
            read = RoleAction(
              Lambda(
                "ref" -> Equals(
                  Identity(),
                  Select(JSArray("data", "user"), Get(Var("ref"))))))))
      } {
        assertDenied(Seq(Get(profileB.refObj)), userA.token)
        assertAllowed(Seq(Get(profileA.refObj)), userA.token)
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
          Membership(
            users,
            Lambda("ref" -> Select(JSArray("data", "admin"), Get(Var("ref"))))),
          Privilege.open(users.refObj))
      } {
        runQuery(
          Update(
            admin.refObj,
            MkObject("data" -> MkObject("admin" -> true))
          ),
          db.adminKey
        )

        runQuery(
          Update(
            normalUser.refObj,
            MkObject("data" -> MkObject("admin" -> false))
          ),
          db.adminKey
        )

        assertDenied(Seq(CreateF(users.refObj)), normalUser.token)
        assertAllowed(Seq(CreateF(users.refObj)), admin.token)
      }
    }

    once("can use auth functions in membership predicates") {
      for {
        db    <- aDatabase
        cls   <- aCollection(db)
        inst  <- aDocument(cls)
        users <- aCollection(db)
        userA <- aUserOfCollection(db, users)
        userB <- aUserOfCollection(db, users)
        _ <- aRole(
          db,
          Membership(users, Lambda("_" -> Equals(Identity(), userA.refObj))),
          Privilege.open(cls.refObj))
      } {
        assertDenied(Seq(Get(inst.refObj)), userB.token)
        assertAllowed(Seq(Get(inst.refObj)), userA.token)
      }
    }
  }

  "tokens - permissions" - {
    once("does not allow change logged in user") {
      for {
        db   <- aDatabase
        user <- aUser(db)
        _ <- aRole(
          db,
          Membership(user.collection),
          Privilege(user.collection.refObj))
      } yield {
        // admin key can update user
        assertAllowed(
          Seq(
            Update(user.refObj)
          ),
          db.adminKey)

        // token
        assertDenied(
          Seq(
            Get(user.refObj),
            Paginate(Events(user.refObj)),
            Update(user.refObj),
            InsertVers(user.refObj, Now(), "create"),
            DeleteF(user.refObj)
          ),
          user.token)

        // document auth
        assertDenied(
          Seq(
            Get(user.refObj),
            Paginate(Events(user.refObj)),
            Update(user.refObj),
            InsertVers(user.refObj, Now(), "create"),
            DeleteF(user.refObj)
          ),
          s"${db.adminKey}:@doc/${user.collection.name}/${user.id}"
        )
      }
    }

    once("allow change logged in user") {
      for {
        db    <- aDatabase
        coll  <- aCollection(db)
        user1 <- aUserOfCollection(db, coll)
        user2 <- aUserOfCollection(db, coll)
        _     <- aRole(db, Membership(coll), Privilege.open(coll.refObj))
      } yield {
        // token
        assertAllowed(
          Seq(
            Get(user1.refObj),
            Paginate(Events(user1.refObj)),
            Update(user1.refObj),
            InsertVers(user1.refObj, Now(), "create"),
            DeleteF(user1.refObj)
          ),
          user1.token)

        // document auth
        assertAllowed(
          Seq(
            Get(user2.refObj),
            Paginate(Events(user2.refObj)),
            Update(user2.refObj),
            InsertVers(user2.refObj, Now(), "create"),
            DeleteF(user2.refObj)
          ),
          s"${db.adminKey}:@doc/${coll.name}/${user2.id}"
        )
      }
    }
  }

  once("allows for unrestricted indexes") {
    for {
      db    <- aDatabase
      cls   <- aCollection(db)
      index <- anIndex(cls, termProp = Prop.const(Seq.empty))
      inst  <- aDocument(cls)
      role <- aRole(
        db,
        Privilege(
          index.refObj,
          unrestrictedRead = RoleAction.Granted
        ))
      key <- aKey(role)
    } yield {
      assertDenied(Seq(Get(inst.refObj)), key.secret)
      (runQuery(Paginate(Match(index.refObj)), key.secret) / "data") should
        containElem(inst.refObj)
    }
  }

  once("allows for unrestricted indexes with predicates") {
    for {
      db    <- aDatabase
      cls   <- aCollection(db)
      index <- anIndex(cls)
      inst <- aDocument(
        cls,
        dataProp = jsObject("foo" -> Prop.const(JSString("foo"))))
      role <- aRole(
        db,
        Privilege(
          index.refObj,
          unrestrictedRead = RoleAction(
            Lambda("terms" ->
              Equals(Var("terms"), JSArray("foo"))))
        ))
      key <- aKey(role)
    } yield {
      assertDenied(
        Seq(
          Get(inst.refObj),
          Paginate(Match(index.refObj, JSArray("bar")))
        ),
        key.secret
      )

      val paginate = Paginate(Match(index.refObj, JSArray("foo")))
      (runQuery(paginate, key.secret) / "data") should containElem(inst.refObj)
    }
  }

  def test(subject: String)(
    mkSecret: (Database, Seq[Privilege]) => Prop[String]): Unit = {

    s"$subject - full table scan" - {
      once("filter documents") {
        for {
          db      <- aDatabase
          cls     <- aCollection(db)
          _       <- aDocument(cls)
          secretA <- mkSecret(db, Seq.empty)
          secretB <- mkSecret(db, Seq(Privilege.open(cls.refObj)))
        } {
          qassert(IsEmpty(Paginate(Documents(cls.refObj))), secretA)
          qassert(IsNonEmpty(Paginate(Documents(cls.refObj))), secretB)
        }
      }

      once("validate unrestricted reads with no predicates") {
        for {
          db  <- aDatabase
          cls <- aCollection(db)
          _   <- aDocument(cls)
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                resource = cls.refObj,
                unrestrictedRead = RoleAction.Granted
              )))
        } {
          qassert(IsNonEmpty(Paginate(Documents(cls.refObj))), secret)
          assertDenied(Seq(Get(Documents(cls.refObj))), secret)
        }
      }

      once("validate unrestricted reads with predicates") {
        for {
          db  <- aDatabase
          cls <- aCollection(db)
          _   <- aDocument(cls)
          secretA <- mkSecret(
            db,
            Seq(
              Privilege(
                resource = cls.refObj,
                unrestrictedRead = RoleAction(Lambda("_" -> false))
              )))
          secretB <- mkSecret(
            db,
            Seq(
              Privilege(
                resource = cls.refObj,
                unrestrictedRead = RoleAction(Lambda("_" -> true))
              )))
        } {
          qassert(IsEmpty(Paginate(Documents(cls.refObj))), secretA)
          qassert(IsNonEmpty(Paginate(Documents(cls.refObj))), secretB)
        }
      }
    }

    s"$subject - instances" - {
      once("validate instance permissions with no predicates") {
        for {
          db     <- aDatabase
          classA <- aCollection(db)
          classB <- aCollection(db)
          instA  <- aDocument(classA)
          instB  <- aDocument(classB)
          secret <- mkSecret(db, Seq(Privilege.open(classA.refObj)))
        } {
          assertDenied(
            Seq(
              Get(instB.refObj),
              Update(instB.refObj),
              Replace(instB.refObj),
              CreateF(classB.refObj),
              CreateF(RefV(123, classB.refObj)),
              DeleteF(instB.refObj),
              InsertVers(instB.refObj, 1, "create"),
              RemoveVers(instB.refObj, 1, "create"),
              Paginate(Events(instB.refObj))
            ),
            secret
          )

          assertAllowed(
            Seq(
              Get(instA.refObj),
              Update(instA.refObj),
              Replace(instA.refObj),
              CreateF(classA.refObj),
              CreateF(RefV(123, classA.refObj)),
              DeleteF(instA.refObj),
              InsertVers(instA.refObj, 1, "create"),
              RemoveVers(instA.refObj, 1, "create"),
              Paginate(Events(instA.refObj))
            ),
            secret
          )
        }
      }

      once("allow write predicates to have access to the ref being written") {
        for {
          db    <- aDatabase
          cls   <- aCollection(db)
          inst1 <- aDocument(cls)
          inst2 <- aDocument(cls)
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                cls.refObj,
                write = RoleAction(Lambda(
                  JSArray("old", "new", "ref") -> Equals(Var("ref"), inst1.refObj)))
              )
            ))
        } {
          assertAllowed(
            Seq(Update(inst1.refObj, MkObject("data" -> MkObject("foo" -> "bar")))),
            secret)
          assertDenied(
            Seq(Update(inst2.refObj, MkObject("data" -> MkObject("foo" -> "bar")))),
            secret)
        }
      }

      once("validate instance permissions with predicates") {
        val mutableField = JSArray("data", "mutable")
        val updateTS = Epoch(1234124124, "second")

        def isMutable(obj: JSValue) =
          Select(mutableField, obj, false)

        for {
          db  <- aDatabase
          cls <- aCollection(db)
          mutableInst <- aDocument(
            cls,
            dataProp = jsObject("mutable" -> Prop.const(JSTrue)))
          immutableInst <- aDocument(
            cls,
            dataProp = jsObject("mutable" -> Prop.const(JSFalse)))
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                cls.refObj,
                write = RoleAction(
                  Lambda(
                    JSArray("old", "new") ->
                      And(
                        isMutable(Var("old")),
                        isMutable(Var("new"))
                      )
                  )),
                read = RoleAction(Lambda("ref" -> isMutable(Get(Var("ref"))))),
                create = RoleAction(Lambda("data" -> isMutable(Var("data")))),
                delete = RoleAction(Lambda("ref" -> isMutable(Get(Var("ref"))))),
                historyRead =
                  RoleAction(Lambda("ref" -> isMutable(Get(Var("ref"))))),
                historyWrite = RoleAction(
                  Lambda(
                    JSArray("ref", "ts", "action", "data") ->
                      If(
                        Equals(Var("action"), "update"),
                        Equals(Var("ts"), updateTS),
                        isMutable(Var("data"))
                      )
                  )
                )
              ))
          )
        } {
          val immutableData = MkObject("data" -> MkObject("mutable" -> false))
          val mutableData = MkObject("data" -> MkObject("mutable" -> true))
          val anyData = MkObject("data" -> MkObject("foo" -> "bar"))

          assertDenied(
            Seq(
              Get(immutableInst.refObj),
              Update(mutableInst.refObj, immutableData),
              Replace(mutableInst.refObj, immutableData),
              Update(immutableInst.refObj, mutableData),
              Replace(immutableInst.refObj, mutableData),
              CreateF(cls.refObj, immutableData),
              CreateF(RefV(123, cls.refObj), immutableData),
              InsertVers(immutableInst.refObj, 1, "update", immutableData),
              RemoveVers(immutableInst.refObj, 1, "update"),
              Paginate(Events(immutableInst.refObj)),
              DeleteF(immutableInst.refObj)
            ),
            secret
          )

          assertAllowed(
            Seq(
              Get(mutableInst.refObj),
              Update(mutableInst.refObj, anyData),
              Replace(mutableInst.refObj, mutableData),
              CreateF(cls.refObj, mutableData),
              CreateF(RefV(123, cls.refObj), mutableData),
              InsertVers(mutableInst.refObj, updateTS, "update", mutableData),
              RemoveVers(mutableInst.refObj, updateTS, "update"),
              Paginate(Events(mutableInst.refObj)),
              DeleteF(mutableInst.refObj)
            ),
            secret
          )
        }
      }

      once("can read instance history without instance read permission") {
        for {
          db   <- aDatabase
          cls  <- aCollection(db)
          inst <- aDocument(cls)
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                cls.refObj,
                read = RoleAction.Denied,
                historyRead = RoleAction.Granted)))
        } yield {
          assertDenied(Seq(Get(inst.refObj)), secret)

          val paginate = Paginate(inst.refObj, events = true)
          (runQuery(paginate, secret) / "data") shouldNot be(empty)
        }
      }
    }

    s"$subject - indexes" - {
      once("validate index permissions with no predicate") {
        for {
          db     <- aDatabase
          classA <- aCollection(db)
          indexA <- anIndex(classA)
          indexB <- anIndex(classA)
          secret <- mkSecret(db, Seq(Privilege.open(indexA.refObj)))
        } {
          assertDenied(Seq(Paginate(Match(indexB.refObj))), secret)
          assertAllowed(Seq(Paginate(Match(indexA.refObj))), secret)
        }
      }

      once("validate index permissions with predicate") {
        for {
          db    <- aDatabase
          cls   <- aCollection(db)
          index <- anIndex(cls)
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                index.refObj,
                read = RoleAction(Lambda("terms" ->
                  Equals(Var("terms"), JSArray("foo"))))
              ))
          )
        } {
          assertDenied(Seq(Paginate(Match(index.refObj, "bar"))), secret)
          assertAllowed(Seq(Paginate(Match(index.refObj, "foo"))), secret)
        }
      }

      once("validate partitioned index permissions with predicate") {
        for {
          db    <- aDatabase
          cls   <- aCollection(db)
          index <- anIndex(cls, partitionsProp = Prop.const(Some(3)))
          role <- aRole(
            db,
            Privilege(
              index.refObj,
              read = RoleAction(
                Lambda("terms" ->
                  Equals(Var("terms"), JSArray("foo"))))
            )
          )
          key <- aKey(role)
        } {
          assertDenied(Seq(Paginate(Match(index.refObj, "bar"))), key.secret)
          assertAllowed(Seq(Paginate(Match(index.refObj, "foo"))), key.secret)
        }
      }

      once("allows non-privileged indexes to be used in predicates") {
        for {
          db     <- aDatabase
          classA <- aCollection(db)
          classB <- aCollection(db)
          indexA <- anIndex(classA)
          indexB <- anIndex(classB)
          _ <- aDocument(
            classA,
            dataProp = jsObject("foo" -> Prop.const(JSString("bar"))))
          _ <- aDocument(
            classB,
            dataProp = jsObject("foo" -> Prop.const(JSString("bar"))))
          role <- aRole(
            db,
            Privilege(
              indexA.refObj,
              read = RoleAction(
                Lambda("terms" ->
                  Exists(Match(indexB.refObj, Select(0, Var("terms"))))))
            )
          )
          key <- aKey(role)
        } {
          assertDenied(
            Seq(
              Paginate(Match(indexB.refObj, "bar")),
              Paginate(Match(indexA.refObj, "baz"))
            ),
            key.secret
          )

          assertAllowed(Seq(Paginate(Match(indexA.refObj, "bar"))), key.secret)
        }
      }
    }

    s"$subject - functions" - {
      once("validate function permissions with no predicate") {
        for {
          db     <- aDatabase
          funcA  <- aFunc(db)
          funcB  <- aFunc(db)
          secret <- mkSecret(db, Seq(Privilege.open(funcA.refObj)))
        } {
          assertDenied(Seq(Call(funcB.refObj)), secret)
          assertAllowed(Seq(Call(funcA.refObj)), secret)
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
                func.refObj,
                call = RoleAction(Lambda("args" ->
                  Equals(Var("args"), JSArray("foo", "bar"))))
              ))
          )
        } {
          assertDenied(Seq(Call(func.refObj, "baz")), secret)
          assertAllowed(Seq(Call(func.refObj, "foo", "bar")), secret)
        }
      }

      once("predicates with read-only user defined functions works") {
        for {
          db        <- aDatabase
          cls       <- aCollection(db)
          inst      <- aDocument(cls)
          pureFunc  <- aFunc(db, Lambda("_" -> JSTrue))
          writeFunc <- aFunc(db, Lambda("_" -> Do(NextID(), JSTrue)))
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                cls.refObj,
                read = RoleAction(Lambda("ref" ->
                  Call(FunctionRef(pureFunc.name)))),
                create = RoleAction(Lambda("data" ->
                  Call(FunctionRef(writeFunc.name))))
              ))
          )
        } {
          assertAllowed(Seq(Get(inst.refObj)), secret)
          assertDenied(Seq(CreateF(cls.refObj)), secret)
        }
      }

      once("user defined functions with roles are restricted to read-only effect") {
        for {
          db        <- aDatabase
          cls       <- aCollection(db)
          inst      <- aDocument(cls)
          pureFunc  <- aFunc(db, Lambda("_" -> JSTrue), role = "admin")
          writeFunc <- aFunc(db, Lambda("_" -> Do(NextID(), JSTrue)), role = "admin")
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                cls.refObj,
                read = RoleAction(Lambda("ref" ->
                  Call(FunctionRef(pureFunc.name)))),
                create = RoleAction(Lambda("data" ->
                  Call(FunctionRef(writeFunc.name))))
              ))
          )
        } {
          assertAllowed(Seq(Get(inst.refObj)), secret)
          assertDenied(Seq(CreateF(cls.refObj)), secret)
        }
      }
    }

    s"$subject - schema" - {
      once("validate schema permissions with no predicate") {
        for {
          db      <- aDatabase
          cls     <- aCollection(db)
          secretA <- mkSecret(db, Seq(Privilege.open(ClassesRef)))
          secretB <- mkSecret(db, Seq.empty)
        } {
          val queries = Seq(
            Get(cls.refObj),
            Paginate(ClassesRef),
            CreateClass(MkObject("name" -> "foo")),
            Update(cls.refObj, MkObject("name" -> "baz"))
          )

          assertDenied(queries :+ DeleteF(cls.refObj), secretB)
          assertAllowed(queries :+ DeleteF(ClassRef("baz")), secretA)
        }
      }

      once("validate schema permissions with predicates") {
        def isNamedFoo(obj: JSValue) =
          Equals(Select(JSArray("name"), obj), "foo")

        for {
          db  <- aDatabase
          cls <- aCollection(db)
          secret <- mkSecret(
            db,
            Seq(
              Privilege(
                ClassesRef,
                create = RoleAction(Lambda("config" -> isNamedFoo(Var("config")))),
                delete = RoleAction(Lambda("ref" -> isNamedFoo(Get(Var("ref"))))),
                read = RoleAction(Lambda("ref" -> isNamedFoo(Get(Var("ref"))))),
                write = RoleAction(Lambda(JSArray("old", "new") ->
                  isNamedFoo(Var("old"))))
              ))
          )
        } {
          assertDenied(
            Seq(
              Get(cls.refObj),
              CreateClass(MkObject("name" -> "bar")),
              Update(cls.refObj, MkObject("name" -> "baz")),
              DeleteF(cls.refObj)
            ),
            secret
          )

          assertAllowed(
            Seq(
              CreateClass(MkObject("name" -> "foo")),
              Get(ClassRef("foo")),
              Update(ClassRef("foo")),
              DeleteF(ClassRef("foo"))
            ),
            secret
          )
        }
      }
    }
  }

  private def assertAllowed(queries: Seq[JSValue], token: String): Unit = {
    foreachWithClue(queries) { query =>
      runRawQuery(query, token) should respond(200, 201)
    }
  }

  private def assertDenied(queries: Seq[JSValue], token: String): Unit = {
    foreachWithClue(queries) { query =>
      runRawQuery(query, token) should respond(403)
    }
  }

  private def foreachWithClue(queries: Seq[JSValue])(fn: JSValue => Any): Unit = {
    queries foreach { query =>
      withClue(s"Query: $query") {
        fn(query)
      }
    }
  }
}
