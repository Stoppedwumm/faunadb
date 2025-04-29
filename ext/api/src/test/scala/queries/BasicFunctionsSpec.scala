package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.prop._
import fauna.prop.api._
import scala.util.Random

class BasicFunctions20Spec extends QueryAPI20Spec {
  "ref constructors" - {
    once("construct refs by looking up names") {
      for {
        db <- aDatabase
        cls <- aFaunaClass(db)
        idx <- anIndex(cls)
        role <- aRole(db, Privilege.open(cls.refObj))
      } {
        val subdb = FaunaDB.makeDB("subdb", client.api, client.cfg.version, adminKey = db.adminKey)
        (runRawQuery(DatabaseRef(db.name), rootKey).json / "resource") should equal(
          Ref(s"databases/${db.name}"))
        runQuery(DatabaseRef(subdb.name), db) should equal(Ref(s"databases/subdb"))
        runQuery(IndexRef(idx.name), db) should equal(Ref(s"indexes/${idx.name}"))
        runQuery(ClassRef(cls.name), db) should equal(Ref(s"classes/${cls.name}"))
        runQuery(RoleRef(role.name), db) should equal(Ref(s"roles/${role.name}"))
      }
    }

    once("constructs native classrefs accessors with null scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aFaunaClass(db)
        idx <- anIndex(cls)
        role <- aRole(db, Privilege.open(cls.refObj))

        user <- aUser(db)
        _    <- aDocument(cls)

        func <- aFunc(db)
      } {
        // Leaving the scope unspecified should yield all instances visible under the supplied key.
        val dbQ = Paginate(DatabaseNativeClassRef)
        val classQ = Paginate(ClassesNativeClassRef)
        val idxQ = Paginate(IndexesNativeClassRef)
        val keysQ = Paginate(KeysNativeClassRef)
        val tokensQ = Paginate(TokensNativeClassRef)
        val credsQ = Paginate(CredentialsNativeClassRef)
        val functionsQ = Paginate(FunctionsNativeClassRef)
        val rolesQ = Paginate(RolesNativeClassRef)

        //Databases
        runQuery(Select(JSArray("data"), dbQ), rootDB.adminKey) should equal(
          JSArray(db.refObj))
        runQuery(Select(JSArray("data"), dbQ), db.adminKey) should equal(
          JSArray(childDB.refObj))
        runQuery(Select(JSArray("data"), dbQ), childDB.adminKey) should equal(
          JSArray())

        //Classes
        runQuery(Select(JSArray("data"), classQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), classQ), db.adminKey) should equal(
          JSArray(cls.refObj, user.collection.refObj))
        runQuery(Select(JSArray("data"), classQ), childDB.adminKey) should equal(
          JSArray())

        //Indexes
        runQuery(Select(JSArray("data"), idxQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), idxQ), db.adminKey) should equal(
          JSArray(idx.refObj))
        runQuery(Select(JSArray("data"), idxQ), childDB.adminKey) should equal(
          JSArray())

        // Functions
        runQuery(Select(JSArray("data"), functionsQ), db.adminKey) should equal(
          JSArray(func.refObj))

        // Roles
        runQuery(Select(JSArray("data"), rolesQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), rolesQ), db.adminKey) should equal(
          JSArray(role.refObj))
        runQuery(Select(JSArray("data"), rolesQ), childDB.adminKey) should equal(
          JSArray())

        // Keys
        val rootKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                                 rootDB.adminKey) / "data").as[JSArray].value
        rootKeys.length should equal(3)
        rootKeys.forall { _ / "database" == db.refObj } should equal(true)
        rootKeys.exists { _ / "role" == JSString("server") } should equal(true)
        rootKeys.exists { _ / "role" == JSString("client") } should equal(true)
        rootKeys.exists { _ / "role" == JSString("admin") } should equal(true)

        val dbKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                               db.adminKey) / "data").as[JSArray].value
        dbKeys.length should equal(3)
        dbKeys.forall { _ / "database" == childDB.refObj } should equal(true)
        dbKeys.exists { _ / "role" == JSString("server") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("client") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("admin") } should equal(true)

        val childKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                                  childDB.adminKey) / "data").as[JSArray]
        childKeys.value.length should equal(0)

        runQuery(Select(JSArray("data"), tokensQ), db) should equal(
          JSArray(user.tokenRes.refObj))
        runQuery(Select(JSArray("data"), credsQ), db) should equal(
          JSArray(user.credential.refObj))
      }
    }

    once("constructs native classrefs accessors with DB ref scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aFaunaClass(db)
        idx <- anIndex(cls)
        role <- aRole(db, Privilege.open(cls.refObj))

        user <- aUser(db)
        _    <- aDocument(cls)

        func <- aFunc(db)
      } {
        // Specifying a scope should yield all databases visible under the ref's scope, or produce an error
        // if that scope is not visible within the supplied Auth.
        val dbQ = Paginate(DatabaseNativeClassRef(DatabaseRef(db.name)))
        val classQ = Paginate(ClassesNativeClassRef(DatabaseRef(db.name)))
        val idxQ = Paginate(IndexesNativeClassRef(DatabaseRef(db.name)))
        val keysQ = Paginate(KeysNativeClassRef(DatabaseRef(db.name)))
        val tokensQ = Paginate(TokensNativeClassRef(DatabaseRef(db.name)))
        val credsQ = Paginate(CredentialsNativeClassRef(DatabaseRef(db.name)))
        val functionsQ = Paginate(FunctionsNativeClassRef(DatabaseRef(db.name)))
        val rolesQ = Paginate(RolesNativeClassRef(DatabaseRef(db.name)))

        runQuery(Select(JSArray("data"), dbQ), rootDB.adminKey) should equal(
          JSArray(childDB.refObj))
        qassertErr(Select(JSArray("data"), dbQ),
                   "invalid ref",
                   JSArray("from", "paginate", "databases"),
                   db.adminKey)

        runQuery(Select(JSArray("data"), classQ), rootDB.adminKey) should equal(
          JSArray(cls.refObj, user.collection.refObj))
        qassertErr(Select(JSArray("data"), classQ),
                   "invalid ref",
                   JSArray("from", "paginate", "classes"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), idxQ), rootDB.adminKey) should equal(
          JSArray(idx.refObj))
        qassertErr(Select(JSArray("data"), idxQ),
                   "invalid ref",
                   JSArray("from", "paginate", "indexes"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), rolesQ), rootDB.adminKey) should equal(
          JSArray(role.refObj))
        qassertErr(Select(JSArray("data"), rolesQ),
                   "invalid ref",
                   JSArray("from", "paginate", "roles"),
                   childDB.adminKey)

        val rootDBKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                                   rootDB.adminKey) / "data").as[JSArray].value
        rootDBKeys.length should equal(3)
        rootDBKeys.forall { _ / "database" == childDB.refObj } should equal(true)
        rootDBKeys.exists { _ / "role" == JSString("server") } should equal(true)
        rootDBKeys.exists { _ / "role" == JSString("client") } should equal(true)
        rootDBKeys.exists { _ / "role" == JSString("admin") } should equal(true)

        val dbKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                               rootDB.adminKey) / "data").as[JSArray].value
        dbKeys.length should equal(3)
        dbKeys.forall { _ / "database" == childDB.refObj } should equal(true)
        dbKeys.exists { _ / "role" == JSString("server") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("client") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("admin") } should equal(true)
        qassertErr(Select(JSArray("data"), keysQ),
                   "invalid ref",
                   JSArray("from", "paginate", "keys"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), functionsQ), rootDB.adminKey) should equal(
          JSArray(func.refObj))

        runQuery(Select(JSArray("data"), tokensQ), rootDB.adminKey) should equal(
          JSArray(user.tokenRes.refObj))

        runQuery(Select(JSArray("data"), credsQ), rootDB.adminKey) should equal(
          JSArray(user.credential.refObj))
      }
    }

    once("constructs native classrefs accessors with non-DB ref scope") {
      for {
        db <- aDatabase(apiVers)
        cls <- aFaunaClass(db)
      } {
        // Arbitrary, but valid refs, cannot be passed to scope
        val dbQ = Paginate(DatabaseNativeClassRef(cls.refObj))
        qassertErr(Select(JSArray("data"), dbQ),
                   "invalid argument",
                   JSArray("from", "paginate", "databases"),
                   db.adminKey)
      }
    }
  }

  "select/contains" - {
    prop("select/contains with no path returns identity") {
      for {
        db <- aDatabase
        v <- jsValue
      } {
        qequals(Select(JSArray(), Quote(v)), Quote(v), db)
        qassert(Contains(JSArray(), Quote(v)), db)
      }
    }

    prop("version contains 'ref', 'ts', 'class', 'data'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = JSObject("paginate" -> set, "size" -> 1)
        val vers = Get(Select(JSArray(0, 1), page))
        val js = runQuery(vers, db)

        qassert(Contains("ref", vers), db)
        qequals(Select("ref", vers), Quote(js / "ref"), db)

        qassert(Contains(JSArray("ref", "class"), vers), db)
        qequals(Select(JSArray("ref", "class"), vers),
                Select("class", Quote(js / "ref")),
                db)

        qassert(Contains("ts", vers), db)
        qequals(Select("ts", vers), Quote(js / "ts"), db)

        qassert(Contains("data", vers), db)
        qequals(Select("data", vers), Quote(js / "data"), db)

        qassert(Contains(JSArray("data", "foo"), vers), db)
        qequals(Select(JSArray("data", "foo"), vers), Quote(js / "data" / "foo"), db)

        qassert(Contains(JSArray("data", "bar"), vers), db)
        qequals(Select(JSArray("data", "bar"), vers), Quote(js / "data" / "bar"), db)
      }
    }

    prop("events contain 'ts', 'action', 'resource'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, events = true)
        val ev = Select(0, page)
        val js = runQuery(ev, db)

        qassert(Contains("ts", ev), db)
        qequals(Select("ts", ev), Quote(js / "ts"), db)

        qassert(Contains("action", ev), db)
        qequals(Select("action", ev), Quote(js / "action"), db)

        qassert(Contains("resource", ev), db)
        qequals(Select("resource", ev), Quote(js / "resource"), db)
      }
    }

    prop("sourced cells contain 'value', 'sources'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, sources = true)
        val cell = Select(0, page)
        val js = runQuery(cell, db)

        qassert(Contains("value", cell), db)
        qequals(Select("value", cell), Quote(js / "value"), db)

        qassert(Contains("sources", cell), db)
        qequals(Select("sources", cell), Quote(js / "sources"), db)
        qequals(Select(JSArray("sources", 0), cell), set, db)
      }
    }

    prop("page contains indexes, 'before', 'after'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db, Prop.const(5))
      } {
        val page = Paginate(set, After(2), size = 2)

        qassert(Contains("before", page), db)
        qequals(Select(JSArray("before", 0), page), 2, db)

        qassert(Contains("after", page), db)
        val after = (runQuery(page, db) / "after" / 0)
        qequals(Select(JSArray("after", 0), page), after, db)

        qassert(Contains(0, page), db)
        qassert(Contains(1, page), db)
        qequals(Contains(2, page), JSFalse, db)
      }
    }

    once("can extract `id`, `class`, `database` from ref") {
      for {
        parentDB <- aDatabase
        db <- aDatabase(apiVers, parentDB)
        cls <- aCollection(db)
        idx <- anIndex(cls)
        inst <- aDocument(cls)
        _ <- mkCredentials(db, inst, "sekrit")
      } {
        val keyRef = runQuery(CreateF(Ref("keys"),
                                      MkObject("role" -> "server",
                                               "database" -> Ref(db.ref))),
                              parentDB.adminKey) / "ref"
        val tokenRef = runQuery(Login(Ref(inst.ref),
                                      MkObject("password" -> "sekrit")),
                                db) / "ref"
        qequals(Select("class", Ref(db.ref)), Ref("databases"), parentDB)
        qequals(Select(JSArray("class", "id"), Ref(db.ref)), "databases", parentDB)

        qequals(Select("class", Ref(cls.ref)), Ref("classes"), db)
        qequals(Select(JSArray("class", "id"), Ref(cls.ref)), "classes", db)

        qequals(Select("class", Ref(idx.ref)), Ref("indexes"), db)
        qequals(Select(JSArray("class", "id"), Ref(idx.ref)), "indexes", db)

        qequals(Select("class", Ref(inst.ref)), Ref(cls.ref), db)
        qequals(Select(JSArray("class", "id"), Ref(inst.ref)), cls.name, db)

        qequals(Select("class", keyRef), Ref("keys"), db)
        qequals(Select(JSArray("class", "id"), keyRef), "keys", db)

        qequals(Select("class", tokenRef), Ref("tokens"), db)
        qequals(Select(JSArray("class", "id"), tokenRef), "tokens", db)

        qequals(Select("id", Ref(db.ref)), db.name, parentDB)
        qequals(Select("id", Ref(cls.ref)), cls.name, db)
        qequals(Select("id", Ref(idx.ref)), idx.name, db)

        val instId = runQuery(Select("id", Ref(inst.ref)), db).as[String]
        qequals(MkRef(Ref(cls.ref), instId), Ref(inst.ref), db)

        val keyId = runQuery(Select("id", keyRef), db).as[String]
        (keyRef / "@ref") should equal(JSString(s"keys/$keyId"))

        val tokenId = runQuery(Select("id", tokenRef), db).as[String]
        (tokenRef / "@ref") should equal(JSString(s"tokens/${tokenId}"))

        qequals(Select("database", ClsRefV(cls.name, DBRefV(db.name))),
                DatabaseRef(db.name),
                parentDB.adminKey)
        qequals(Select(JSArray("database", "id"),
                       ClsRefV(cls.name, DBRefV(db.name))),
                db.name,
                parentDB.adminKey)
        qequals(Select(JSArray("database", "database", "id"),
                       ClsRefV(cls.name, DBRefV(db.name, DBRefV(parentDB.name)))),
                parentDB.name,
                rootKey)

        qassertErr(Select("database", DatabaseRef(db.name)),
                   "value not found",
                   "Value not found at path [database].",
                   JSArray("from"),
                   parentDB.adminKey)

        qequals(Select(JSArray("ref", "class"),
                       CreateClass(MkObject("name" -> "fooclass"))),
                Ref("classes"),
                db)
        qassertErr(Select("id", ClassRef("invalid-ref")),
                   "invalid ref",
                   "Ref refers to undefined class 'invalid-ref'",
                   JSArray("from"),
                   db)
      }
    }

    once("can extract `id` and `class` from native class refs") {
      for {
        db <- aDatabase
      } {
        val native =
          Set("databases", "classes", "keys", "tokens", "indexes", "credentials")
        native foreach { c =>
          qequals(Select("class", Ref(c)), Ref("classes"), db)
          qequals(Select("id", Ref(c)), c, db)
        }
      }
    }

    once("can extract from set definitions") {
      for {
        db <- aDatabase
      } {
        val refsR = List(Ref("databases"), Ref("classes"))
        Seq(
          ("union", Union(refsR: _*)),
          ("intersection", Intersection(refsR: _*)),
          ("difference", Difference(refsR: _*))
        ) foreach {
          case (name, set) =>
            qequals(Select(JSArray("@set", name), set), refsR, db)
            qequals(Select(JSArray("@set", name, 0), set), refsR.head, db)
            qequals(Select(JSArray("@set", name, 0, "id"), set), "databases", db)
            qequals(Select(JSArray("@set", name, 0, "class"), set),
                    Ref("classes"),
                    db)
        }

        qequals(Select(JSArray("@set", "distinct"), Distinct(refsR.head)),
                refsR.head,
                db)
        qequals(Select(JSArray("@set", "distinct", "id"), Distinct(refsR.head)),
                "databases",
                db)
        qequals(Select(JSArray("@set", "distinct", "class"), Distinct(refsR.head)),
                Ref("classes"),
                db)

        val joinIdx = Join(Ref("databases"), Lambda("v" -> Var("v")))
        qequals(Select(JSArray("@set", "join"), joinIdx), Ref("databases"), db)
        qequals(Select(JSArray("@set", "with"), joinIdx),
                QueryF(Lambda("v" -> Var("v"))),
                db)

        val cls = aCollection(db).sample
        val idx = anIndex(cls).sample
        val idxSet = Match(Ref(idx.ref), "foo")
        qequals(Select(JSArray("@set", "match"), idxSet), Ref(idx.ref), db)
        qequals(Select(JSArray("@set", "terms"), idxSet), "foo", db)

        qequals(Select(JSArray("@set", "terms"),
                       Match(Ref(idx.ref), Seq("foo", "bar"))),
                Seq("foo", "bar"),
                db)
        qequals(Select(JSArray("@set", "terms", 1),
                       Match(Ref(idx.ref), Seq("foo", "bar"))),
                "bar",
                db)
      }
    }
  }

  "paginate" - {
    once("invalid refs on cursors") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Paginate(ClassesNativeClassRef, After(ClassRef("foo"))),
          "invalid ref",
          "Ref refers to undefined class 'foo'",
          JSArray("after"),
          db
        )

        qassertErr(
          Paginate(ClassesNativeClassRef, Before(ClassRef("foo"))),
          "invalid ref",
          "Ref refers to undefined class 'foo'",
          JSArray("before"),
          db
        )
      }
    }
  }
}

class BasicFunctions21Spec extends QueryAPI21Spec {
  "abort" - {
    once("aborts the transaction") {
      for {
        db <- aDatabase
      } {
        qassertErr(Abort("<%= reason %>"), "transaction aborted", JSArray(), db.key)
      }
    }
  }

  "new_id" - {
    once("makes ids") {
      for {
        db <- aDatabase
      } {
        runQuery(NewID, db.key)
      }
    }
  }

  "ref" - {
    once("makes refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(MkRef(cls.refObj, NewID), db.key)
        qassertErr(Get(MkRef(cls.refObj, NewID)),
                   "instance not found",
                   JSArray(),
                   db.key)
        runQuery(Let("id" -> MkRef(cls.refObj, NewID)) {
          Do(
            InsertVers(Var("id"), 1, "create"),
            Get(Var("id"))
          )
        }, db.key)
      }
    }

    once("rejects native classes") {
      for {
        db <- aDatabase
      } {
        qassertErr(MkRef(Ref("databases"), "foo"),
                   "invalid argument",
                   JSArray("ref"),
                   db.key)
        qassertErr(MkRef(Ref("classes"), "people"),
                   "invalid argument",
                   JSArray("ref"),
                   db.key)
        qassertErr(MkRef(Ref("indexes"), "people_by_name"),
                   "invalid argument",
                   JSArray("ref"),
                   db.key)
        runQuery(MkRef(Ref("keys"), "1"), db.key)
        runQuery(MkRef(Ref("tokens"), "1"), db.key)
      }
    }
  }

  "select/contains" - {
    prop("events contain 'ts', 'action', 'instance'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, events = true)
        val ev = Select(0, page)
        val js = runQuery(ev, db)

        qassert(Contains("ts", ev), db)
        qequals(Select("ts", ev), Quote(js / "ts"), db)

        qassert(Contains("action", ev), db)
        qequals(Select("action", ev), Quote(js / "action"), db)

        qassert(Contains("instance", ev), db)
        qequals(Select("instance", ev), Quote(js / "instance"), db)

        qequals(Select(JSArray("instance", "id"), ev),
          Quote(js / "instance" / "@ref" / "id"), db)
        qequals(Select(JSArray("instance", "class", "id"), ev),
          Quote(js / "instance" / "@ref" / "class" / "@ref" / "id"), db)
      }
    }

    prop("doc events contain 'data'") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls, dataProp = jsObject(1))
      } {
        val page = Paginate(Events(inst.refObj), size = 1)
        val ev = Select(0, page)
        val js = runQuery(ev, db)

        qassert(Contains("data", ev), db)
        qequals(Select("data", ev), Quote(js / "data"), db)

        val field = Random.choose((js / "data").as[JSObject].keys)
        qequals(Select(JSArray("data", field), ev), Quote(js / "data" / field), db)
      }
    }

    prop("set events contain 'data'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(Events(set), size = 1)
        val ev = Select(0, page)
        val js = runQuery(ev, db)

        qassert(Contains("data", ev), db)
        qequals(Select("data", ev), Quote(js / "data"), db)

        qassert(Contains(JSArray("data", 0), ev), db)
        qequals(Select(JSArray("data", 0), ev), Quote(js / "data" / 0), db)
      }
    }
  }

  "ref constructors" - {
    once("construct refs by looking up names") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        role <- aRole(db, Privilege.open(cls.refObj))
      } {
        val subdb = FaunaDB.makeDB("subdb", client.api, client.cfg.version, adminKey = db.adminKey)
        (runRawQuery(DatabaseRef(db.name), rootKey).json / "resource") should equal(
          RefV(db.name, RefV("databases")))
        runQuery(DatabaseRef(subdb.name), db) should equal(
          RefV("subdb", RefV("databases")))
        runQuery(IndexRef(idx.name), db) should equal(
          RefV(idx.name, RefV("indexes")))
        runQuery(ClassRef(cls.name), db) should equal(
          RefV(cls.name, RefV("classes")))
        runQuery(RoleRef(role.name), db) should equal(RefV(role.name, RefV("roles")))
      }
    }

    once("constructs native classrefs accessors with null scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aCollection(db)
        idx <- anIndex(cls)

        user <- aUser(db)
        _    <- aDocument(cls)

        func <- aFunc(db)
      } {
        // Leaving the scope unspecified should yield all instances visible under the supplied key.
        val dbQ = Paginate(DatabaseNativeClassRef)
        val classQ = Paginate(ClassesNativeClassRef)
        val idxQ = Paginate(IndexesNativeClassRef)
        val keysQ = Paginate(KeysNativeClassRef)
        val tokensQ = Paginate(TokensNativeClassRef)
        val credsQ = Paginate(CredentialsNativeClassRef)
        val functionsQ = Paginate(FunctionsNativeClassRef)

        //Databases
        runQuery(Select(JSArray("data"), dbQ), rootDB.adminKey) should equal(
          JSArray(db.refObj))
        runQuery(Select(JSArray("data"), dbQ), db.adminKey) should equal(
          JSArray(childDB.refObj))
        runQuery(Select(JSArray("data"), dbQ), childDB.adminKey) should equal(
          JSArray())

        //Classes
        runQuery(Select(JSArray("data"), classQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), classQ), db.adminKey) should equal(
          JSArray(cls.refObj, user.collection.refObj))
        runQuery(Select(JSArray("data"), classQ), childDB.adminKey) should equal(
          JSArray())

        //Indexes
        runQuery(Select(JSArray("data"), idxQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), idxQ), db.adminKey) should equal(
          JSArray(idx.refObj))
        runQuery(Select(JSArray("data"), idxQ), childDB.adminKey) should equal(
          JSArray())

        // Functions
        runQuery(Select(JSArray("data"), functionsQ), db.adminKey) should equal(
          JSArray(func.refObj))

        // Keys

        val rootKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                                 rootDB.adminKey) / "data").as[JSArray].value
        rootKeys.length should equal(3)
        rootKeys.forall { _ / "database" == db.refObj } should equal(true)
        rootKeys.exists { _ / "role" == JSString("server") } should equal(true)
        rootKeys.exists { _ / "role" == JSString("client") } should equal(true)
        rootKeys.exists { _ / "role" == JSString("admin") } should equal(true)

        val dbKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                               db.adminKey) / "data").as[JSArray].value
        dbKeys.length should equal(3)
        dbKeys.forall { _ / "database" == childDB.refObj } should equal(true)
        dbKeys.exists { _ / "role" == JSString("server") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("client") } should equal(true)
        dbKeys.exists { _ / "role" == JSString("admin") } should equal(true)

        val childKeys = (runQuery(MapF(Lambda("r" -> Get(Var("r"))), keysQ),
                                  childDB.adminKey) / "data").as[JSArray]
        childKeys.value.length should equal(0)

        runQuery(Select(JSArray("data"), tokensQ), db.adminKey) should equal(
          JSArray(user.tokenRes.refObj))

        runQuery(Select(JSArray("data"), credsQ), db.adminKey) should equal(
          JSArray(user.credential.refObj))
      }
    }

    once("constructs native classrefs accessors with DB ref scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aCollection(db)
        idx <- anIndex(cls, Prop.const(Seq.empty))
        user <- aUser(db)
        doc <- aDocument(cls)

        role <- aRole(db)
        func <- aFunc(db)
      } {
        // Specifying a scope should yield all databases visible under the ref's scope, or produce an error
        // if that scope is not visible within the supplied Auth.
        val dbQ = Paginate(DatabaseNativeClassRef(DatabaseRef(db.name)))
        val classQ = Paginate(ClassesNativeClassRef(DatabaseRef(db.name)))
        val idxQ = Paginate(IndexesNativeClassRef(DatabaseRef(db.name)))
        val keysQ = Paginate(KeysNativeClassRef(DatabaseRef(db.name)))
        val rolesQ = Paginate(RolesNativeClassRef(DatabaseRef(db.name)))
        val tokensQ = Paginate(TokensNativeClassRef(DatabaseRef(db.name)))
        val credsQ = Paginate(CredentialsNativeClassRef(DatabaseRef(db.name)))
        val functionsQ = Paginate(FunctionsNativeClassRef(DatabaseRef(db.name)))

        runQuery(Select(JSArray("data"), dbQ), rootDB.adminKey) should equal(
          JSArray(DBRefV(childDB.name, db.refObj)))
        qassertErr(Select(JSArray("data"), dbQ),
                   "invalid ref",
                   JSArray("from", "paginate", "databases"),
                   db.adminKey)

        runQuery(Select(JSArray("data"), classQ), rootDB.adminKey) should equal(
          JSArray(ClsRefV(cls.name, db.refObj), ClsRefV(user.collection.name, db.refObj)))
        qassertErr(Select(JSArray("data"), classQ),
                   "invalid ref",
                   JSArray("from", "paginate", "classes"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), idxQ), rootDB.adminKey) should equal(
          JSArray(IdxRefV(idx.name, db.refObj)))
        qassertErr(Select(JSArray("data"), idxQ),
                   "invalid ref",
                   JSArray("from", "paginate", "indexes"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), functionsQ), rootDB.adminKey) should equal(
          JSArray(FnRefV(func.name, db.refObj)))
        qassertErr(Select(JSArray("data"), functionsQ),
                   "invalid ref",
                   JSArray("from", "paginate", "functions"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), rolesQ), rootDB.adminKey) should equal(
          JSArray(RoleRefV(role.name, db.refObj)))
        qassertErr(Select(JSArray("data"), rolesQ),
             "invalid ref",
                   JSArray("from", "paginate", "roles"),
                   childDB.adminKey)

        val adminKey = runQuery(KeyFromSecret(childDB.adminKey), rootDB.adminKey)
        val clientKey = runQuery(KeyFromSecret(childDB.clientKey), rootDB.adminKey)
        val serverKey = runQuery(KeyFromSecret(childDB.key), rootDB.adminKey)

        runQuery(Select(JSArray("data"), keysQ), rootDB.adminKey).as[JSArray].value.toSet should equal(Set(
          RefV(serverKey.id, RefV("keys", None, Some(db.refObj))),
          RefV(clientKey.id, RefV("keys", None, Some(db.refObj))),
          RefV(adminKey.id, RefV("keys", None, Some(db.refObj)))))

        qassertErr(Select(JSArray("data"), keysQ),
                   "invalid ref",
                   JSArray("from", "paginate", "keys"),
                   childDB.adminKey)

        runQuery(Select(JSArray("data"), tokensQ), rootDB.adminKey) should equal(
          JSArray(RefV(user.tokenRes.id, RefV("tokens", None, Some(db.refObj)))))

        runQuery(Select(JSArray("data"), credsQ), rootDB.adminKey) should equal(
          JSArray(RefV(user.credential.id, RefV("credentials", None, Some(db.refObj)))))

        runQuery(Select("data", Paginate(Match(IndexRef(idx.name, DatabaseRef(db.name))))), rootDB) should equal(
          JSArray(RefV(doc.id, ClsRefV(cls.name, db.refObj))))
      }
    }

    once("native refs are rendered properly") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
      } {
        val scopeRef = DatabaseRef(db.name)
        runQuery(DatabaseRef("aDB", scopeRef), rootDB) should equal(
          RefV("aDB", DatabasesRef, db.refObj))
        runQuery(ClassRef("aClass", scopeRef), rootDB) should equal(
          RefV("aClass", ClassesRef, db.refObj))
        runQuery(IndexRef("anIndex", scopeRef), rootDB) should equal(
          RefV("anIndex", IndexesRef, db.refObj))
        runQuery(FunctionRef2("aFunction", scopeRef), rootDB) should equal(
          RefV("aFunction", FunctionsRef, db.refObj))

        runQuery(DatabaseRef("aDB"), rootDB) should equal(RefV("aDB", DatabasesRef))
        runQuery(ClassRef("aClass"), rootDB) should equal(RefV("aClass", ClassesRef))
        runQuery(IndexRef("anIndex"), rootDB) should equal(
          RefV("anIndex", IndexesRef))
        runQuery(FunctionRef("aFunction"), rootDB) should equal(
          RefV("aFunction", FunctionsRef))
        runQuery(RoleRef("aRole"), rootDB) should equal(RefV("aRole", RolesRef))
      }
    }

    once("native classrefs doesn't lose scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
      } {
        runQuery(DatabaseNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("databases", None, Some(db.refObj))
        runQuery(ClassesNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("classes", None, Some(db.refObj))
        runQuery(IndexesNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("indexes", None, Some(db.refObj))
        runQuery(KeysNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("keys", None, Some(db.refObj))
        runQuery(FunctionsNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("functions", None, Some(db.refObj))
        runQuery(RolesNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("roles", None, Some(db.refObj))
        runQuery(CredentialsNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("credentials", None, Some(db.refObj))
        runQuery(TokensNativeClassRef(DatabaseRef(db.name)), rootDB) shouldBe RefV("tokens", None, Some(db.refObj))
      }
    }

    once("constructs native classrefs accessors with non-DB ref scope") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        // Arbitrary, but valid refs, cannot be passed to scope
        val dbQ = Paginate(DatabaseNativeClassRef(cls.refObj))
        qassertErr(Select(JSArray("data"), dbQ),
                   "invalid argument",
                   JSArray("from", "paginate", "databases"),
                   db.adminKey)
      }
    }
  }

  "equals" - {
    prop("json literals equal themselves") {
      for {
        db <- aDatabase
        js <- jsValue
      } {
        qassert(Equals(Quote(js), Quote(js)), db)
      }
    }

    prop("object key order does not matter") {
      for {
        db <- aDatabase
        js1 <- jsObject
        js2 <- Prop.shuffle(js1.value) map { JSObject(_: _*) }
      } {
        qassert(Equals(Quote(js1), Quote(js2)), db)
      }
    }

    prop("number comparison") {
      for {
        db <- aDatabase
        x <- Prop.long
      } {
        qassert(Equals(JSLong(x), JSLong(x)), db)
        qassert(Equals(JSDouble(x.toDouble), JSDouble(x.toDouble)), db)
        qassert(Equals(JSDouble(x.toDouble), JSLong(x), JSDouble(x.toDouble)), db)
      }
    }

    prop("a version equals itself") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1)
        val vers = Get(Select(JSArray(0, 1), page))

        qequals(vers, vers, db)
      }
    }

    prop("an event equals itself") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, events = true)
        val ev = Select(0, page)

        qequals(ev, ev, db)
      }
    }

    prop("a sourced cell equals itself") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, sources = true)
        val cell = Select(0, page)

        qequals(cell, cell, db)
      }
    }

    prop("a page equals itself") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set)

        qequals(page, page, db)
      }
    }

    once("update actions are considered equals to the 'create' String") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("foo" -> "bar")))
      } {
        runQuery(Update(doc.refObj, MkObject("data" -> MkObject("foo" -> "baz"))), db)

        val paginateEvent = Paginate(Events(doc.refObj))
        val events = runQuery(paginateEvent, db)
        val data = events.get("data").asInstanceOf[JSArray].value
        // validate actions beforehand
        data.head.get("action") should equal(JSString("create"))
        data(1).get("action") should equal(JSString("update"))

        // compute equality of event.action fields against "create" String
        val equalsCreateSelectActions = MapF(
          Lambda("event" ->
            Equals(
              "create",
              Select("action", Var("event"))
            )
          ),
          paginateEvent
        )
        val r1 = runQuery(equalsCreateSelectActions, db)
        // the update event is not matching "create"
        r1 should equal(JSObject("data" -> JSArray(true, true)))

        // compute equality of event.action fields against "update" String
        val equalsUpdateSelectActions = MapF(
          Lambda("event" ->
            Equals(
              "update",
              Select("action", Var("event"))
            )
          ),
          paginateEvent
        )
        val r2 = runQuery(equalsUpdateSelectActions, db)
        // the create event is not matching "update"
        r2 should equal(JSObject("data" -> JSArray(false, true)))
      }
    }
  }

  "select_all" - {
    val obj = MkObject(
      "data" -> MkObject(
        "arr" -> JSArray(1, JSArray(2, JSArray(3)), 4, MkObject("foo" -> "bar"))))
    val res = JSArray(1, 2, 3, 4, MkObject("foo" -> "bar"))

    once("works") {
      for {
        db <- aDatabase
      } {
        qequals(SelectAll(JSArray("data", "arr"), obj), res, db)
        qequals(SelectAll(JSArray("data", "foo"), obj), JSArray(), db)

        val obj2 = MkObject(
          "data" -> JSArray(MkObject("bar" -> 1),
                            MkObject("bar" -> 2),
                            MkObject("bar" -> JSArray(3, JSArray(4, JSArray(5))))))
        val res2 = JSArray(1, 2, 3, 4, 5)
        qequals(SelectAll(JSArray("data", "bar"), obj2), res2, db)
      }
    }

    once("is equivalent to select(..., all=true)") {
      for {
        db <- aDatabase
      } {
        val paths =
          List(JSArray("data"), JSArray("data", "arr"), JSArray("data", "foo"))

        for (p <- paths) {
          qequals(SelectAll(p, obj), Select(p, obj, JSArray(), Some(true)), db)
        }
      }
    }

    once("is equivalent to SelectAsIndex") {
      for {
        db <- aDatabase
      } {
        val paths =
          List(JSArray("data"), JSArray("data", "arr"), JSArray("data", "foo"))

        for (p <- paths) {
          qequals(SelectAll(p, obj), SelectAsIndex(p, obj), db)
        }
      }
    }

  }

  "events" - {
    // this test does not claim to assert how things should be, it only documents the current state of affairs
    once("noop updates result in additional events") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("foo" -> "bar")))
      } {
        runQuery(Update(doc.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)
        runQuery(Update(doc.refObj, MkObject("data" -> MkObject("foo" -> "bar"))), db)

        val events = runQuery(Paginate(Events(doc.refObj)), db)
        events.get("data").asInstanceOf[JSArray].value.size should equal(3)
      }
    }
  }
}

class BasicFunctions27Spec extends QueryAPI27Spec {
  "ref" - {
    once("makes refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        runQuery(MkRef(cls.refObj, NewID), db.key)
        qassertErr(Get(MkRef(cls.refObj, NewID)),
          "instance not found",
          JSArray(),
          db.key)
        runQuery(Let("id" -> MkRef(cls.refObj, NewID)) {
          Do(
            InsertVers(Var("id"), 1, "create"),
            Get(Var("id"))
          )
        }, db.key)
      }
    }

    once("rejects native classes") {
      for {
        db <- aDatabase
      } {
        qassertErr(MkRef(Ref("databases"), "foo"),
          "invalid argument",
          JSArray("ref"),
          db.key)
        qassertErr(MkRef(Ref("collections"), "people"),
          "invalid argument",
          JSArray("ref"),
          db.key)
        qassertErr(MkRef(Ref("indexes"), "people_by_name"),
          "invalid argument",
          JSArray("ref"),
          db.key)
        runQuery(MkRef(Ref("keys"), "1"), db.key)
        runQuery(MkRef(Ref("tokens"), "1"), db.key)
      }
    }
  }

  "select/contains" - {
    prop("events contain 'ts', 'action', 'document'") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        val page = Paginate(set, size = 1, events = true)
        val ev = Select(0, page)
        val js = runQuery(ev, db)

        qassert(Contains("ts", ev), db)
        qequals(Select("ts", ev), Quote(js / "ts"), db)

        qassert(Contains("action", ev), db)
        qequals(Select("action", ev), Quote(js / "action"), db)

        qassert(Contains("document", ev), db)
        qequals(Select("document", ev), Quote(js / "document"), db)

        qequals(Select(JSArray("document", "id"), ev),
          Quote(js / "document" / "@ref" / "id"), db)
        qequals(Select(JSArray("document", "collection", "id"), ev),
          Quote(js / "document" / "@ref" / "collection" / "@ref" / "id"), db)
      }
    }

    once("default value is not lazy") {
      for {
        db <- aDatabase
      } {
        qassertErr(
          Select("x", MkObject("x" -> JSTrue), Abort("msg")),
          "transaction aborted",
          "msg",
          JSArray("default"),
          db
        )
      }
    }

    once("cannot read data through refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        other <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "bar")))
        doc <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "baz", "other" -> other.refObj)))
      } {
        val docRef = doc.refObj

        qassertErr(Select(JSArray("ref"), docRef), "value not found", "Value not found at path [ref].", JSArray("from"), db)
        qassertErr(Select(JSArray("ts"), docRef), "value not found", "Value not found at path [ts].", JSArray("from"), db)
        qassertErr(Select(JSArray("data"), docRef), "value not found", "Value not found at path [data].", JSArray("from"), db)
        qassertErr(Select(JSArray("data", "foo"), docRef), "value not found", "Value not found at path [data,foo].", JSArray("from"), db)
        qassertErr(Select(JSArray("data", "other"), docRef), "value not found", "Value not found at path [data,other].", JSArray("from"), db)
        qassertErr(Select(JSArray("data", "other", "data"), docRef), "value not found", "Value not found at path [data,other,data].", JSArray("from"), db)
        qassertErr(Select(JSArray("data", "other", "data", "foo"), docRef), "value not found", "Value not found at path [data,other,data,foo].", JSArray("from"), db)

        qequals(Contains(JSArray("ref"), docRef), JSFalse, db)
        qequals(Contains(JSArray("ts"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data", "foo"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data", "other"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data", "other", "data"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data", "other", "data", "foo"), docRef), JSFalse, db)

        val invalidRef = MkRef(cls.refObj, "1")

        qequals(Select(JSArray("ref"), invalidRef, "default value"), "default value", db)
        qequals(Select(JSArray("ts"), invalidRef, "default value"), "default value", db)
        qequals(Select(JSArray("data"), invalidRef, "default value"), "default value", db)

        qequals(Contains(JSArray("ref"), docRef), JSFalse, db)
        qequals(Contains(JSArray("ts"), docRef), JSFalse, db)
        qequals(Contains(JSArray("data"), docRef), JSFalse, db)
      }
    }
  }

  "ref constructors" - {
    once("construct refs by looking up names") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
        role <- aRole(db, Privilege.open(cls.refObj))
      } {
        val subdb = FaunaDB.makeDB("subdb", client.api, client.cfg.version, adminKey = db.adminKey)
        (runRawQuery(DatabaseRef(db.name), rootKey).json / "resource") should equal(
          RefV(db.name, RefV("databases")))
        runQuery(DatabaseRef(subdb.name), db) should equal(
          RefV("subdb", RefV("databases")))
        runQuery(IndexRef(idx.name), db) should equal(
          RefV(idx.name, RefV("indexes")))
        runQuery(ClassRef(cls.name), db) should equal(
          RefV(cls.name, RefV("collections")))
        runQuery(RoleRef(role.name), db) should equal(RefV(role.name, RefV("roles")))
      }
    }

    once("constructs native classrefs accessors with null scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aCollection(db)
      } {
        // Leaving the scope unspecified should yield all instances visible under the supplied key.
        val collectionQ = Paginate(ClassesNativeClassRef)

        //Collections
        runQuery(Select(JSArray("data"), collectionQ), rootDB.adminKey) should equal(
          JSArray())
        runQuery(Select(JSArray("data"), collectionQ), db.adminKey) should equal(
          JSArray(cls.refObj))
        runQuery(Select(JSArray("data"), collectionQ), childDB.adminKey) should equal(
          JSArray())
      }
    }

    once("constructs native classrefs accessors with DB ref scope") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
        childDB <- aDatabase(apiVers, db)

        cls <- aCollection(db)
      } {
        // Specifying a scope should yield all databases visible under the ref's scope, or produce an error
        // if that scope is not visible within the supplied Auth.
        val collectionQ = Paginate(ClassesNativeClassRef(DatabaseRef(db.name)))

        runQuery(Select(JSArray("data"), collectionQ), rootDB.adminKey) should equal(
          JSArray(ClsRefV(cls.name, db.refObj)))
        qassertErr(Select(JSArray("data"), collectionQ),
          "invalid ref",
          JSArray("from", "paginate", "collections"),
          childDB.adminKey)
      }
    }

    once("native refs are rendered properly") {
      for {
        rootDB <- aDatabase
        db <- aDatabase(apiVers, rootDB)
      } {
        val scopeRef = DatabaseRef(db.name)
        runQuery(ClassRef("aClass", scopeRef), rootDB) should equal(
          RefV("aClass", ClassesRef, db.refObj))
        runQuery(ClassRef("aClass"), rootDB) should equal(RefV("aClass", ClassesRef))
      }
    }
  }
}

class BasicFunctions3Spec extends QueryAPI3Spec {

  "select" - {
    once("default value is lazy") {
      for {
        db <- aDatabase
      } {
        qequals(
          Select("x", MkObject("x" -> JSTrue), Abort("msg")),
          JSTrue,
          db
        )

        qassertErr(
          Select("y", MkObject("x" -> JSTrue), Abort("msg")),
          "transaction aborted",
          "msg",
          JSArray("default"),
          db
        )
      }
    }

    once("return default value when reference is invalid") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val invalidRef = MkRef(cls.refObj, "1")

        qequals(Select(JSArray("ref"), invalidRef, "default value"), "default value", db)
        qequals(Select(JSArray("ts"), invalidRef, "default value"), "default value", db)
        qequals(Select(JSArray("data"), invalidRef, "default value"), "default value", db)
      }
    }
  }

  "contains_path/contains_field" - {
    prop("contains_path with no path returns identity") {
      for {
        db <- aDatabase
        v <- jsValue
      } {
        qassert(ContainsPath(JSArray(), Quote(v)), db)
      }
    }

    once("contains_path/contains_field extract fields from objects") {
      for {
        db <- aDatabase
      } {
        qassert(ContainsPath("foo", MkObject("foo" -> "bar")), db)
        qassert(ContainsField("foo", MkObject("foo" -> "bar")), db)

        qassert(ContainsPath(JSArray("foo", "bar"), MkObject("foo" -> MkObject("bar" -> "baz"))), db)
      }
    }

    once("contains_field doesn't accept path or indexes or arrays") {
      for {
        db <- aDatabase
        array <- jsArray
      } {
        qassertErr(
          ContainsField(JSArray("foo"), MkObject()),
          "invalid argument",
          "String expected, Array provided.",
          JSArray("contains_field"),
          db
        )

        qassertErr(
          ContainsField(0, MkObject()),
          "invalid argument",
          "String expected, Integer provided.",
          JSArray("contains_field"),
          db
        )

        qassertErr(
          ContainsField("foo", Quote(array)),
          "invalid argument",
          "Object expected, Array provided.",
          JSArray("in"),
          db
        )
      }
    }

    once("doesn't contains path/field when reference is invalid") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
      } {
        val invalidRef = MkRef(cls.refObj, "1")

        qequals(ContainsField("ref", invalidRef), JSFalse, db)
        qequals(ContainsField("ts", invalidRef), JSFalse, db)
        qequals(ContainsField("data", invalidRef), JSFalse, db)

        qequals(ContainsPath(JSArray("ref"), invalidRef), JSFalse, db)
        qequals(ContainsPath(JSArray("ts"), invalidRef), JSFalse, db)
        qequals(ContainsPath(JSArray("data"), invalidRef), JSFalse, db)
      }
    }
  }

  "contains_value" - {
    prop("refs") {
      for {
        db <- aDatabase
        sub <- aDatabase(apiVers, db)
        coll <- aCollection(sub)
        doc <- aDocument(coll)
      } {
        val ref = MkRef(ClassRef(coll.name, sub.refObj), doc.id)

        //id
        qassert(ContainsValue(doc.id, ref), db)
        //collection
        qassert(ContainsValue(ClassRef(coll.name, sub.refObj), ref), db)
        //database
        qassert(ContainsValue(sub.refObj, ref), db)

        qequals(ContainsValue("10", ref), JSFalse, db)
      }
    }

    once("arrays") {
      for {
        db <- aDatabase
      } {
        qassert(ContainsValue(1, JSArray(0, 1, 2)), db)
        qassert(ContainsValue(JSArray(1), JSArray(JSArray(0), JSArray(1), JSArray(2))), db)
        qassert(ContainsValue(MkObject("foo" -> "bar"), JSArray(MkObject("foo" -> "bar"), MkObject("foo" -> "baz"))), db)

        qequals(ContainsValue(10, JSArray(0, 1, 2)), JSFalse, db)
      }
    }

    once("objects") {
      for {
        db <- aDatabase
      } {
        qassert(ContainsValue(1, MkObject("x" -> 1, "y" -> 2)), db)
        qassert(ContainsValue(JSArray(1), MkObject("x" -> JSArray(1), "y" -> JSArray(2))), db)
        qassert(ContainsValue(MkObject("foo" -> "bar"), MkObject("x" -> MkObject("foo" -> "bar"), "y" -> MkObject("foo" -> "baz"))), db)

        qequals(ContainsValue(10, MkObject("x" -> 1, "y" -> 2)), JSFalse, db)
      }
    }

    once("sets with refs") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, termProp = Prop.const(Seq(JSArray("data", "value"))))
        foo <- aDocument(coll, dataProp = Prop.const(JSObject("value" -> "foo")))
        bar <- aDocument(coll, dataProp = Prop.const(JSObject("value" -> "bar")))
      } {
        val fooSet = Match(idx.refObj, "foo")
        val barSet = Match(idx.refObj, "bar")

        qequals(ContainsValue(foo.refObj, fooSet), JSTrue, db)
        qequals(ContainsValue(bar.refObj, fooSet), JSFalse, db)

        qequals(ContainsValue(foo.refObj, barSet), JSFalse, db)
        qequals(ContainsValue(bar.refObj, barSet), JSTrue, db)

        qequals(ContainsValue("str", barSet), JSFalse, db)
        qequals(ContainsValue(100, barSet), JSFalse, db)
      }
    }

    once("sets with covered values") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, termProp = Prop.const(Seq(JSArray("data", "value"))), valueProp = Prop.const(Seq((JSArray("data", "value"), false))))
        foo <- aDocument(coll, dataProp = Prop.const(JSObject("value" -> "foo")))
        bar <- aDocument(coll, dataProp = Prop.const(JSObject("value" -> "bar")))
      } {
        val fooSet = Match(idx.refObj, "foo")
        val barSet = Match(idx.refObj, "bar")

        qequals(ContainsValue("foo", fooSet), JSTrue, db)
        qequals(ContainsValue("bar", fooSet), JSFalse, db)

        qequals(ContainsValue("foo", barSet), JSFalse, db)
        qequals(ContainsValue("bar", barSet), JSTrue, db)

        qequals(ContainsValue(foo.refObj, barSet), JSFalse, db)
        qequals(ContainsValue(bar.refObj, barSet), JSFalse, db)
      }
    }

    once("sets with multiple covered values") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx0 <- anIndex(coll, termProp = Prop.const(Nil), valueProp = Prop.const(Seq((JSArray("data", "x"), false), (JSArray("data", "y"), false))))
        idx1 <- anIndex(coll, termProp = Prop.const(Nil), valueProp = Prop.const(Seq((JSArray("data", "x"), false), (JSArray("data", "y"), false), ("ref", false))))
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("x" -> 10, "y" -> 20)))
      } {
        val set0 = Match(idx0.refObj)
        val set1 = Match(idx1.refObj)

        qequals(ContainsValue(JSArray(10, 20), set0), JSTrue, db)
        qequals(ContainsValue(10, set0), JSFalse, db)
        qequals(ContainsValue(20, set0), JSFalse, db)

        qequals(ContainsValue(JSArray(10, 20, doc.refObj), set1), JSTrue, db)
        qequals(ContainsValue(JSArray(10, 20), set1), JSFalse, db)
      }
    }

    once("sets with role constraints") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, termProp = Prop.const(Nil))
        doc0 <- aDocument(coll)
        doc1 <- aDocument(coll)
        role <- aRole(
          db,
          Privilege(idx.refObj, read = RoleAction.Granted),
          Privilege(coll.refObj, read = RoleAction(Lambda("ref" -> Equals(Var("ref"), doc0.refObj))))
        )
      } {
        qequals(ContainsValue(doc0.refObj, Match(idx.refObj)), JSTrue, s"${db.adminKey}:@role/${role.name}")
        qequals(ContainsValue(doc1.refObj, Match(idx.refObj)), JSFalse, s"${db.adminKey}:@role/${role.name}")
      }
    }

    prop("scalar") {
      for {
        db <- aDatabase
        value <- jsScalarValue
      } {
        qassertErr(
          ContainsValue(value, value),
          "invalid argument",
          "Object, Array, Page, Set, or Ref expected, Scalar provided.",
          JSArray("in"),
          db
        )
      }
    }

    once("version") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("foo" -> "bar")))
      } {
        val version = Get(doc.refObj)

        //ts
        qassert(ContainsValue(doc.ts, version), db)

        //ref
        qassert(ContainsValue(doc.refObj, version), db)

        //class
        qassert(ContainsValue(coll.refObj, version), db)

        //data
        qassert(ContainsValue(MkObject("foo" -> "bar"), version), db)
      }
    }

    prop("pages") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, termProp = Prop.const(Nil))
        doc0 <- aDocument(coll)
        doc1 <- aDocument(coll)
        doc2 <- aDocument(coll)
      } {
        val page = Paginate(Match(idx.refObj), size = 2, cursor = After(doc0.refObj))

        //data
        qassert(ContainsValue(JSArray(doc0.refObj, doc1.refObj), page), db)

        //before
        qassert(ContainsValue(JSArray(doc0.refObj), page), db)

        //after
        qassert(ContainsValue(JSArray(doc2.refObj), page), db)
      }
    }

    prop("doc events") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
      } {
        val docEvent = InsertVers(doc.refObj, Now(), "create", MkObject("data" -> MkObject("foo" -> "bar")))

        //ts
        qassert(ContainsValue(ToMicros(Now()), docEvent), db)

        //action
        qassert(ContainsValue("create", docEvent), db)

        //resource/instance/document
        qassert(ContainsValue(doc.refObj, docEvent), db)

        //data
        qassert(ContainsValue(MkObject("foo" -> "bar"), docEvent), db)
      }
    }

    prop("permission denied") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        user <- aUser(db)
        _ <- aRole(db, Membership(user.collection), Privilege(idx.refObj, read = RoleAction.Denied))
      } {
        qassertErr(
          ContainsValue("foo", Match(idx.refObj)),
          "permission denied",
          "Insufficient privileges to perform the action.",
          JSArray.empty,
          user.token
        )
      }
    }
  }
}

class BasicFunctions4Spec extends QueryAPI4Spec {
  //todo: Once we get to api version 4, we should move all Contains() tests to here
  // and replace its usage to ContainsPath()
}

class BasicFunctions5Spec extends QueryAPI5Spec {
  "equals" - {
    once("update actions are not considered equals to the 'create' String'") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("foo" -> "bar")))
      } {
        runQuery(Update(doc.refObj, MkObject("data" -> MkObject("foo" -> "baz"))), db)

        val paginateEvent = Paginate(Events(doc.refObj))
        val events = runQuery(paginateEvent, db)
        val data = events.get("data").asInstanceOf[JSArray].value
        // validate actions beforehand
        data.head.get("action") should equal(JSString("create"))
        data(1).get("action") should equal(JSString("update"))

        // compute equality of event.action fields against "create" String
        val equalsCreateSelectActions = MapF(
          Lambda("event" ->
            Equals(
              "create",
              Select("action", Var("event"))
            )
          ),
          paginateEvent
        )
        val r1 = runQuery(equalsCreateSelectActions, db)
        // the update event is not matching "create"
        r1 should equal(JSObject("data" -> JSArray(true, false)))

        // compute equality of event.action fields against "update" String
        val equalsUpdateSelectActions = MapF(
          Lambda("event" ->
            Equals(
              "update",
              Select("action", Var("event"))
            )
          ),
          paginateEvent
        )
        val r2 = runQuery(equalsUpdateSelectActions, db)
        // the create event is not matching "update"
        r2 should equal(JSObject("data" -> JSArray(false, true)))
      }
    }
  }
}

class BasicFunctionsUnstableSpec extends QueryAPIUnstableSpec {
  "select" - {
    once("can read data through refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        other <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "bar")))
        doc <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "baz", "other" -> other.refObj)))
      } {
        val docRef = doc.refObj

        qequals(Select(JSArray("ref"), docRef), docRef, db)
        qequals(Select(JSArray("ts"), docRef), doc.ts, db)
        qequals(Select(JSArray("data"), docRef), MkObject("foo" -> "baz", "other" -> other.refObj), db)
        qequals(Select(JSArray("data", "foo"), docRef), "baz", db)

        qequals(Select(JSArray("data", "other"), docRef), other.refObj, db)
        qequals(Select(JSArray("data", "other", "ref"), docRef), other.refObj, db)
        qequals(Select(JSArray("data", "other", "ts"), docRef), other.ts, db)
        qequals(Select(JSArray("data", "other", "data"), docRef), MkObject("foo" -> "bar"), db)
        qequals(Select(JSArray("data", "other", "data", "foo"), docRef), "bar", db)
      }
    }
  }

  "contains_path/contains_field" - {
    once("can read data through refs") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        other <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "bar")))
        doc <- aDocument(cls, dataProp = Prop.const(JSObject("foo" -> "baz", "other" -> other.refObj)))
      } {
        val docRef = doc.refObj

        qassert(ContainsField("ref", docRef), db)
        qassert(ContainsField("ts", docRef), db)

        qassert(ContainsPath(JSArray("ref"), docRef), db)
        qassert(ContainsPath(JSArray("ts"), docRef), db)
        qassert(ContainsPath(JSArray("data"), docRef), db)
        qassert(ContainsPath(JSArray("data", "foo"), docRef), db)

        qassert(ContainsPath(JSArray("data", "other"), docRef), db)
        qassert(ContainsPath(JSArray("data", "other", "ref"), docRef), db)
        qassert(ContainsPath(JSArray("data", "other", "ts"), docRef), db)
        qassert(ContainsPath(JSArray("data", "other", "data"), docRef), db)
        qassert(ContainsPath(JSArray("data", "other", "data", "foo"), docRef), db)
      }
    }
  }
}