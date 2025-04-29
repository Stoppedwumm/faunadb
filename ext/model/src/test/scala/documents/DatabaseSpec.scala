package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.schema.{ PublicCollection, SchemaCollection, SchemaStatus }
import fauna.repo.query.Query
import fauna.repo.store.LookupStore
import fauna.repo.test.CassandraHelper
import fauna.repo.Store
import fauna.storage.{ Add, DocAction, Resolved, Unresolved }
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.storage.ir.{ IRValue, NullV }
import fauna.storage.lookup._
import fauna.storage.ops.VersionAdd
import org.scalactic.source
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class DatabaseSpec extends Spec {
  import SocialHelpers._

  "DatabaseSpec" - {
    val ctx = CassandraHelper.context("model")

    val scope = ctx ! newScope(RootAuth, AccountID(Random.nextInt()))
    val dbName = (ctx ! Database.forScope(scope)).get.name

    // make sure this is populated
    ctx ! SchemaStatus.pinActiveSchema(scope, SchemaVersion.Min)

    def dbVersQ =
      Database
        .idByName(Database.RootScopeID, dbName)
        .flatMapT(id => Store.getUnmigrated(Database.RootScopeID, id.toDocID))
        .map(_.get)
    def databaseQ =
      Database
        .idByName(Database.RootScopeID, dbName)
        .flatMapT(Database.getUncached(Database.RootScopeID, _))
        .map(_.get)
    def getScopeQ = databaseQ map { _.scopeID }
    def getGlobalIDQ = databaseQ map { _.globalID }

    "hides the scope on render" in {
      val readable = ReadAdaptor.readableData(ctx ! dbVersQ)
      readable.fields.contains(List("scope")) should be(false)
    }

    "hides the scope on contains" in {
      val contains = runQuery(
        RootAuth,
        Clock.time,
        Contains(JSString("scope"), Get(Ref(s"databases/${dbName}"))))
      (ctx ! contains) should equal(FalseL)
    }

    "hides the scope on select" in {
      val select = evalQuery(
        RootAuth,
        Clock.time,
        Select(JSString("scope"), Get(Ref(s"databases/${dbName}"))))
      val res = (ctx ! select).left.value
      res should have size (1)
      res(0) shouldBe a[ValueNotFound]
    }

    "disallows the scope to be set via query" in {
      val init = (ctx ! getScopeQ)
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Update(Ref(s"databases/${dbName}"), MkObject("scope" -> 0)))
      (ctx ! getScopeQ) should equal(init)
    }

    "disallows the scope to be altered via replace" in {
      val init = (ctx ! getScopeQ)
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Replace(Ref(s"databases/${dbName}"), Quote(JSObject())))
      (ctx ! getScopeQ) should equal(init)
    }

    "account defaults to root" in {
      val dbQ = runQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject("name" -> Random.nextInt().toString, "container" -> true)))

      val version = (ctx ! dbQ) match {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      version.data(Database.AccountField) should be(None)

      val db =
        ctx ! Database.getUncached(version.parentScopeID, version.docID.as[DatabaseID])
      db.get.accountID should equal(Database.DefaultAccount)
    }

    "handles account settings without id" in {
      val emptyAcctIDQ =
        CreateDatabase(
          MkObject(
            "name" -> Random.nextInt().toString,
            "container" -> true,
            "account" -> MkObject("not-id" -> 123)))

      val emptyAcctID = ctx ! runQuery(RootAuth, Clock.time, emptyAcctIDQ).map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      noException should be thrownBy {
        emptyAcctID.data(Database.AccountField)
      }

      emptyAcctID.data(Database.AccountField) shouldNot be(None)
      emptyAcctID.data(Database.AccountIDField) should be(None)
    }

    "hides account" in {
      // has an account to hide
      val version = ctx ! dbVersQ
      version.data(Database.AccountField) shouldNot be(None)

      val readable = ReadAdaptor.readableData(ctx ! dbVersQ)
      readable.fields.contains(Database.AccountField.path) should be(false)
    }

    "hides account on contains" in {
      val contains = runQuery(
        RootAuth,
        Clock.time,
        Contains(
          Database.AccountField.path map { JSString(_) },
          Get(DatabaseRef(dbName))))
      (ctx ! contains) should equal(FalseL)
    }

    "hides account on select" in {
      val select = evalQuery(
        RootAuth,
        Clock.time,
        Select(
          Database.AccountField.path map { JSString(_) },
          Get(DatabaseRef(dbName))))
      val res = (ctx ! select).left.value
      res should have size (1)
      res(0) shouldBe a[ValueNotFound]
    }

    "disallows account update" in {
      val init = ctx ! dbVersQ.map(_.data)
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Update(
          DatabaseRef(dbName),
          MkObject("account" -> MkObject("id" -> ToNumber(NewID())))))
      (ctx ! dbVersQ.map(_.data)) should equal(init)
    }

    "disallows account replacement" in {
      val init = ctx ! dbVersQ.map(_.data)
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Replace(DatabaseRef(dbName), Quote(JSObject())))
      (ctx ! dbVersQ.map(_.data)) should equal(init)
    }

    "inherits account" in {
      val parentQ = runQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> "parent_db",
            "container" -> true,
            "account" -> MkObject("id" -> ToNumber(NewID()))))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val parent = ctx ! parentQ
      val auth = Auth.adminForScope(parent.data(Database.ScopeField))

      val childQ = runQuery(
        auth,
        Clock.time,
        CreateDatabase(MkObject("name" -> "child_db"))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val version = ctx ! childQ

      val db =
        ctx ! Database.getUncached(version.parentScopeID, version.docID.as[DatabaseID])
      Some(db.get.accountID) should equal(parent.data(Database.AccountIDField))
    }

    "cannot create a database with a duplicate account" in {
      val dbQ = runQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> "original",
            "container" -> true,
            "account" -> MkObject("id" -> ToNumber(NewID()))))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val db = ctx ! dbQ
      val account = db.data(Database.AccountIDField).get.toLong

      val result = ctx ! evalQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> "dupe",
            "container" -> true,
            "account" -> MkObject("id" -> account))))

      result.isLeft should be(true)
      result.swap foreach { errs =>
        errs should contain(
          InstanceAlreadyExists(db.docID, Position("create_database")))
      }
    }

    "cannot override inherited account" in {
      val parentQ = runQuery(
        RootAuth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> "account_db",
            "container" -> true,
            "account" -> MkObject("id" -> ToNumber(NewID()))))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val parent = ctx ! parentQ
      val auth = Auth.adminForScope(parent.data(Database.ScopeField))

      val childQ = runQuery(
        auth,
        Clock.time,
        CreateDatabase(
          MkObject(
            "name" -> "override_db",
            "account" -> MkObject("id" -> ToNumber(NewID()))))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unexpected: $r")
      }

      val version = ctx ! childQ

      val db =
        ctx ! Database.getUncached(version.parentScopeID, version.docID.as[DatabaseID])
      Some(db.get.accountID) should equal(parent.data(Database.AccountIDField))
    }

    "is indexed into LookupStore" in {
      val scope = ctx ! newScope
      val dbName = (ctx ! Database.forScope(scope)).get.name

      (ctx ! Store.getDatabaseID(scope)).isDefined should be(true)
      ctx ! evalQuery(RootAuth, Clock.time, DeleteF(Ref(s"databases/${dbName}")))
      (ctx ! Store.getDatabaseID(scope)).isEmpty should be(true)
    }

    "detects scope loops" in {
      val scope = ctx ! newScope
      ctx ! LookupStore.add(
        LiveLookup(scope, scope, DatabaseID(0).toDocID, Unresolved, Add))
      a[ScopeLoopException] should be thrownBy { ctx ! Database.forScope(scope) }
    }

    "concurrent creation" in {
      val futs = 1 to 10 map { i =>
        ctx
          .withRetryOnContention(maxAttempts = 10)
          .runNow(
            runQuery(RootAuth, CreateDatabase(MkObject("name" -> s"concurrent$i"))))
      }

      Await.result(futs.join, 30.seconds)

      val ids = 1 to 10 map { i =>
        (ctx ! runQuery(RootAuth, Get(DatabaseRef(s"concurrent$i")))) match {
          case VersionL(vers, _) => vers.docID.subID.toLong
          case _                 => fail()
        }
      }

      ids.distinct.size should equal(10)
    }

    "can be switched to container when empty" in {
      val dbName = "okContainer"

      ctx ! runQuery(RootAuth, CreateDatabase(MkObject("name" -> dbName)))
      ctx ! runQuery(
        RootAuth,
        Update(DatabaseRef(dbName), MkObject("container" -> true)))

      // the flag should flip
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField
            .read(vers.data.fields)
            .toOption
            .flatten
            .fold(fail()) { _ should be(true) }
        case _ => fail()
      }
    }

    "can be switched to container with a role" in {
      val dbName = "okContainerRoles"
      val roleName = "myRoleName"

      val childVers = (ctx ! runQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> dbName)))) match {
        case VersionL(vers, _) => vers
        case _                 => fail()
      }
      val childAuth = Auth.adminForScope(Database.ScopeField(childVers.data.fields))

      ctx ! runQuery(
        childAuth,
        CreateRole(
          MkObject("name" -> roleName, "privileges" -> JSArray())
        )
      )

      ctx ! runQuery(
        RootAuth,
        Update(DatabaseRef(dbName), MkObject("container" -> true)))

      // the flag should flip
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField
            .read(vers.data.fields)
            .toOption
            .flatten
            .fold(fail()) { _ should be(true) }
        case _ => fail()
      }
    }

    "can be switched to container with a key" in {
      val dbName = "okContainerKey"
      val innerDbName = "inner"

      val childVers = (ctx ! runQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> dbName)))) match {
        case VersionL(vers, _) => vers
        case _                 => fail()
      }
      val childAuth = Auth.adminForScope(Database.ScopeField(childVers.data.fields))
      ctx ! runQuery(childAuth, CreateDatabase(MkObject("name" -> innerDbName)))
      ctx ! mkKey(innerDbName, "server", childAuth)
      ctx ! runQuery(
        RootAuth,
        Update(DatabaseRef(dbName), MkObject("container" -> true)))

      // the flag should flip
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField
            .read(vers.data.fields)
            .toOption
            .flatten
            .fold(fail()) { _ should be(true) }
        case _ => fail()
      }
    }

    "cannot be switched to container with an index" in {
      val dbName = "badContainerIdx"
      val className = "animals"

      val childVers = (ctx ! runQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> dbName)))) match {
        case VersionL(vers, _) => vers
        case _                 => fail()
      }
      val childAuth = Auth.forScope(Database.ScopeField(childVers.data.fields))

      ctx ! mkCollection(childAuth, MkObject("name" -> className))

      val data = JSObject(
        "name" -> "buildme",
        "active" -> true,
        "source" -> Ref(s"classes/$className"),
        "terms" -> JSArray(JSObject("field" -> List("data", "name"))))

      ctx ! runQuery(childAuth, CreateIndex(Quote(data)))

      try {
        ctx ! runQuery(
          RootAuth,
          Update(DatabaseRef(dbName), MkObject("container" -> true)))
        fail() // shouldn't get here
      } catch {
        case e: Throwable =>
          e.getMessage.contains("ContainerCandidateContainsData") should be(true)
      }

      // and not flip the flag
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField.read(vers.data.fields).toOption.flatten map {
            _ should be(false)
          }
        case _ => fail()
      }
    }

    "cannot be switched to container with a collection" in {
      val dbName = "badContainerCol"
      val className = "animals"

      val childVers = (ctx ! runQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> dbName)))) match {
        case VersionL(vers, _) => vers
        case _                 => fail()
      }
      val childAuth = Auth.forScope(Database.ScopeField(childVers.data.fields))

      ctx ! mkCollection(childAuth, MkObject("name" -> className))

      try {
        ctx ! runQuery(
          RootAuth,
          Update(DatabaseRef(dbName), MkObject("container" -> true)))
        fail() // shouldn't get here
      } catch {
        case e: Throwable =>
          e.getMessage.contains("ContainerCandidateContainsData") should be(true)
      }

      // and not flip the flag
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField.read(vers.data.fields).toOption.flatten map {
            _ should be(false)
          }
        case _ => fail()
      }
    }

    "cannot be switched to container with a UDF" in {
      val dbName = "badContainerUDF"

      val childVers = (ctx ! runQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> dbName)))) match {
        case VersionL(vers, _) => vers
        case _                 => fail()
      }
      val childAuth = Auth.forScope(Database.ScopeField(childVers.data.fields))

      ctx ! runQuery(
        childAuth,
        CreateFunction(
          MkObject(
            "name" -> "id",
            "body" -> QueryF(Lambda("x" -> Var("x")))
          )))

      // should throw exception
      try {
        ctx ! runQuery(
          RootAuth,
          Update(DatabaseRef(dbName), MkObject("container" -> true)))
        fail() // shouldn't get here
      } catch {
        case e: Throwable =>
          e.getMessage.contains("ContainerCandidateContainsData") should be(true)
      }

      // and not flip the flag
      (ctx ! runQuery(RootAuth, Get(DatabaseRef(dbName)))) match {
        case VersionL(vers, _) =>
          Database.ContainerField.read(vers.data.fields).toOption.flatten map {
            _ should be(false)
          }
        case _ => fail()
      }
    }

    "database already exists" in {
      val db = (ctx ! runQuery(
        RootAuth,
        Select("ref", CreateDatabase(MkObject("name" -> "somedb")))))
        .asInstanceOf[RefL]
      (ctx ! evalQuery(
        RootAuth,
        CreateDatabase(MkObject("name" -> "somedb")))) match {
        case Left(errors) =>
          errors shouldBe List(
            InstanceAlreadyExists(db.id, RootPosition at "create_database"))
        case Right(_) => fail()
      }
    }

    "renders globalID" in {
      val get = runQuery(RootAuth, Clock.time, Get(DatabaseRef(dbName))) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unknown result: $r")
      }

      val readable = ReadAdaptor.readableData(ctx ! get)
      readable.fields.contains(List("global_id")) should be(true)
    }

    "disallows the globalID to be set via query" in {
      val init = ctx ! getGlobalIDQ
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Update(DatabaseRef(dbName), MkObject("global_id" -> "ignore")))
      (ctx ! getGlobalIDQ) shouldBe init
    }

    "disallows the globalID to be altered via replace" in {
      val init = ctx ! getGlobalIDQ
      ctx ! runQuery(
        RootAuth,
        Clock.time,
        Replace(DatabaseRef(dbName), MkObject("global_id" -> "ignore")))
      (ctx ! getGlobalIDQ) shouldBe init
    }

    "allow globalID in select/contains" in {
      val globalID = ctx ! getGlobalIDQ

      (ctx ! runQuery(
        RootAuth,
        Clock.time,
        Contains("global_id", Get(DatabaseRef(dbName))))) shouldBe TrueL
      (ctx ! runQuery(
        RootAuth,
        Clock.time,
        Select("global_id", Get(DatabaseRef(dbName))))) shouldBe StringL(
        Database.encodeGlobalID(globalID))
    }

    "setActiveSchemaVersion sets the active_schema_version field" in {
      try {
        (ctx ! dbVersQ).data(Database.ActiveSchemaVersField) shouldEqual Some(0L)

        ctx ! SchemaStatus.clearActiveSchema(scope)

        (ctx ! dbVersQ).data(Database.ActiveSchemaVersField) shouldEqual None

      } finally {
        // set this back
        ctx ! SchemaStatus.pinActiveSchema(scope, SchemaVersion.Min)
      }
    }

    Seq(
      "native_schema_version" -> Database.NativeSchemaVersField,
      "active_schema_version" -> Database.ActiveSchemaVersField).foreach {
      case (name, field) =>
        s"hides $name on render" in {
          // has a vers to hide
          val version = ctx ! dbVersQ
          version.data(field) shouldNot be(None)

          val readable = ReadAdaptor.readableData(ctx ! dbVersQ)
          readable.fields.contains(field.path) should be(false)
        }

        s"hides $name on contains" in {
          val contains = runQuery(
            RootAuth,
            Clock.time,
            Contains(field.path map { JSString(_) }, Get(DatabaseRef(dbName))))
          (ctx ! contains) should equal(FalseL)
        }

        s"hides $name on select" in {
          val select = evalQuery(
            RootAuth,
            Clock.time,
            Select(field.path map { JSString(_) }, Get(DatabaseRef(dbName))))
          val res = (ctx ! select).left.value
          res should have size (1)
          res(0) shouldBe a[ValueNotFound]
        }

        s"disallows $name to be set via update" in {
          val init = ctx ! dbVersQ.map(_.data)
          ctx ! runQuery(
            RootAuth,
            Clock.time,
            Update(DatabaseRef(dbName), MkObject(name -> 123)))
          (ctx ! dbVersQ.map(_.data)) should equal(init)
        }

        s"disallows $name to be altered via replace" in {
          val init = ctx ! dbVersQ.map(_.data)
          ctx ! runQuery(
            RootAuth,
            Clock.time,
            Replace(DatabaseRef(dbName), MkObject(name -> 123)))
          (ctx ! dbVersQ.map(_.data)) should equal(init)
        }
    }

    "scoped ref" in {
      val child = ctx ! newScope
      val childAuth = Auth.adminForScope(child)
      val db = ctx ! Database.forScope(child)

      ctx ! runQuery(childAuth, CreateCollection(MkObject("name" -> "coll")))

      // remove lookup entry so Database.forScope returns None
      val remove = Store.getLatestLookup(db.get.scopeID) flatMapT { entry =>
        LookupStore.remove(entry) map { Some(_) }
      }

      ctx ! remove

      val render =
        RenderContext.apply(RootAuth, APIVersion.Default, Clock.time) flatMap {
          context =>
            runQuery(
              RootAuth,
              ClassesNativeClassRef(DatabaseRef(db.get.name))) flatMap { ref =>
              context.render(ref) map { JSON.parse[JSValue](_) }
            }
        }

      (ctx ! render) shouldBe JSNull
    }

    "databases at root of an account can be retrieved by account ID" in {
      def dbQ(auth: Auth, obj: JSObject) =
        runQuery(auth, Clock.time, CreateDatabase(obj)) map {
          case VersionL(v, _) => v
          case r              => sys.error(s"Unexpected: $r")
        }

      // Set up a simple hierarchy that resembles the production one:
      //
      // (root)
      //  |
      // <root_db>---=-+
      //  |            |
      // <alice_db>*  <bob_db>*
      //  |
      // <alice_child_db>
      //
      // *'d databases are assigned account IDs.
      val rootQ = dbQ(RootAuth, MkObject("name" -> "root_db", "container" -> true))

      val root = ctx ! rootQ
      val auth = Auth.adminForScope(root.data(Database.ScopeField))

      val accountAliceQ = dbQ(
        auth,
        MkObject(
          "name" -> "alice_db",
          "account" -> MkObject("id" -> ToNumber(NewID()))))

      val accountAlice = ctx ! accountAliceQ
      val aliceAuth = Auth.adminForScope(accountAlice.data(Database.ScopeField))

      val childAliceQ = dbQ(aliceAuth, MkObject("name" -> "alice_child_db"))
      val childAlice = ctx ! childAliceQ

      val accountBobQ = dbQ(
        auth,
        MkObject(
          "name" -> "bob_db",
          "account" -> MkObject("id" -> ToNumber(NewID()))))
      val accountBob = ctx ! accountBobQ

      // Both alice_db and alice_child_db should have the same account ID...
      val aliceDB = ctx ! Database.getUncached(
        accountAlice.parentScopeID,
        accountAlice.docID.as[DatabaseID])
      val childAliceDB =
        ctx ! Database.getUncached(childAlice.parentScopeID, childAlice.docID.as[DatabaseID])
      (aliceDB.get.accountID) should equal(childAliceDB.get.accountID)

      // ...but alice_db's scope should be the one retrieved from the index using the
      // account ID.
      val alice = ctx ! Database.forAccount(childAliceDB.get.accountID)
      alice should equal(Some(aliceDB.get))

      // bob_db's scope can be retrieved by its account ID, too.
      val bobDB =
        ctx ! Database.getUncached(accountBob.parentScopeID, accountBob.docID.as[DatabaseID])
      val bob = ctx ! Database.forAccount(bobDB.get.accountID)
      bob should equal(Some(bobDB.get))
    }

    "database delete" - {
      def newDB(parent: ScopeID) =
        (ctx ! Database.getUncached(ctx ! newScope(Auth.adminForScope(parent)))).get

      def lookupShouldBePresent(db: Database)(implicit pos: source.Position) =
        (ctx ! Store.getDatabaseID(db.scopeID)) shouldEqual Some(
          (db.parentScopeID, db.id))

      def lookupShouldNotBePresent(db: Database)(implicit pos: source.Position) =
        (ctx ! Store.getDatabaseID(db.scopeID)) shouldEqual None

      "does not leave dangling live lookups of descendants" - {
        "parent, child" in {
          // create a parent and child
          val db1 = newDB(ScopeID.RootID)
          val db2 = newDB(db1.scopeID)

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)

          ctx ! runQuery(RootAuth, DeleteF(DatabaseRef(db1.name)))

          lookupShouldNotBePresent(db1)
          lookupShouldNotBePresent(db2)
        }

        "parent, child, grandchild" in {
          // create a parent, child, gchild
          val db1 = newDB(ScopeID.RootID)
          val db2 = newDB(db1.scopeID)
          val db3 = newDB(db2.scopeID)

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)
          lookupShouldBePresent(db3)

          ctx ! runQuery(RootAuth, DeleteF(DatabaseRef(db1.name)))

          lookupShouldNotBePresent(db1)
          lookupShouldNotBePresent(db2)
          lookupShouldNotBePresent(db3)
        }
      }

      "only deletes its subtree" - {
        "1->2, 1->3, delete 2" in {
          val db1 = newDB(ScopeID.RootID)
          // children
          val db2 = newDB(db1.scopeID)
          val db3 = newDB(db1.scopeID)

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)
          lookupShouldBePresent(db3)

          ctx ! runQuery(
            Auth.adminForScope(db1.scopeID),
            DeleteF(DatabaseRef(db2.name)))

          lookupShouldBePresent(db1)
          lookupShouldNotBePresent(db2)
          lookupShouldBePresent(db3)
        }
        "1->2, 1->3, 2->4, 3->5, delete 2" in {
          val db1 = newDB(ScopeID.RootID)
          // children
          val db2 = newDB(db1.scopeID)
          val db3 = newDB(db1.scopeID)
          // grandchildren
          val db4 = newDB(db2.scopeID)
          val db5 = newDB(db3.scopeID)

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)
          lookupShouldBePresent(db3)
          lookupShouldBePresent(db4)
          lookupShouldBePresent(db5)

          ctx ! runQuery(
            Auth.adminForScope(db1.scopeID),
            DeleteF(DatabaseRef(db2.name)))

          lookupShouldBePresent(db1)
          lookupShouldNotBePresent(db2)
          lookupShouldBePresent(db3)
          lookupShouldNotBePresent(db4)
          lookupShouldBePresent(db5)
        }

        "1->2, 1->3, 2->4, 2->5, delete 4" in {
          val db1 = newDB(ScopeID.RootID)
          // children
          val db2 = newDB(db1.scopeID)
          val db3 = newDB(db1.scopeID)
          // grandchildren
          val db4 = newDB(db2.scopeID)
          val db5 = newDB(db2.scopeID)

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)
          lookupShouldBePresent(db3)
          lookupShouldBePresent(db4)
          lookupShouldBePresent(db5)

          ctx ! runQuery(
            Auth.adminForScope(db2.scopeID),
            DeleteF(DatabaseRef(db4.name)))

          lookupShouldBePresent(db1)
          lookupShouldBePresent(db2)
          lookupShouldBePresent(db3)
          lookupShouldNotBePresent(db4)
          lookupShouldBePresent(db5)
        }
      }
    }

    "getLiveGlobalIDsInHierarchy" - {
      def newDB(parent: ScopeID) =
        (ctx ! Database.getUncached(ctx ! newScope(Auth.adminForScope(parent)))).get

      "reports back on the hierarchy" in {
        val db1 = newDB(ScopeID.RootID)
        // children
        val db2 = newDB(db1.scopeID)
        val db3 = newDB(db1.scopeID)
        // grandchildren
        val db4 = newDB(db2.scopeID)
        val db5 = newDB(db2.scopeID)

        // deleted subtree
        val db6 = newDB(db1.scopeID)
        newDB(db6.scopeID)

        ctx ! runQuery(
          Auth.adminForScope(db1.scopeID),
          DeleteF(DatabaseRef(db6.name)))

        (ctx ! Database.getUncached(db6.scopeID)).get.isDeleted shouldEqual true

        val ids1 =
          ctx ! Database.getLiveGlobalIDsInHierarchy(
            ScopeID.RootID,
            DatabaseID.RootID)
        val ids1Map = ids1.toMap

        ids1Map(db1.scopeID) shouldEqual ((db1.parentScopeID, db1.id))
        ids1Map(db2.scopeID) shouldEqual ((db2.parentScopeID, db2.id))
        ids1Map(db3.scopeID) shouldEqual ((db3.parentScopeID, db3.id))
        ids1Map(db4.scopeID) shouldEqual ((db4.parentScopeID, db4.id))
        ids1Map(db5.scopeID) shouldEqual ((db5.parentScopeID, db5.id))
        ids1Map(db1.globalID) shouldEqual ((db1.parentScopeID, db1.id))
        ids1Map(db2.globalID) shouldEqual ((db2.parentScopeID, db2.id))
        ids1Map(db3.globalID) shouldEqual ((db3.parentScopeID, db3.id))
        ids1Map(db4.globalID) shouldEqual ((db4.parentScopeID, db4.id))
        ids1Map(db5.globalID) shouldEqual ((db5.parentScopeID, db5.id))

        val ids2 =
          ctx ! Database.getLiveGlobalIDsInHierarchy(db1.parentScopeID, db1.id)
        val ids2Map = ids2.toMap

        ids2.size shouldEqual 10
        ids2Map(db1.scopeID) shouldEqual ((db1.parentScopeID, db1.id))
        ids2Map(db2.scopeID) shouldEqual ((db2.parentScopeID, db2.id))
        ids2Map(db3.scopeID) shouldEqual ((db3.parentScopeID, db3.id))
        ids2Map(db4.scopeID) shouldEqual ((db4.parentScopeID, db4.id))
        ids2Map(db5.scopeID) shouldEqual ((db5.parentScopeID, db5.id))
        ids2Map(db1.globalID) shouldEqual ((db1.parentScopeID, db1.id))
        ids2Map(db2.globalID) shouldEqual ((db2.parentScopeID, db2.id))
        ids2Map(db3.globalID) shouldEqual ((db3.parentScopeID, db3.id))
        ids2Map(db4.globalID) shouldEqual ((db4.parentScopeID, db4.id))
        ids2Map(db5.globalID) shouldEqual ((db5.parentScopeID, db5.id))

        val ids3 =
          ctx ! Database.getLiveGlobalIDsInHierarchy(db2.parentScopeID, db2.id)
        val ids3Map = ids3.toMap

        ids3.size shouldEqual 6
        ids3Map(db2.scopeID) shouldEqual ((db2.parentScopeID, db2.id))
        ids3Map(db4.scopeID) shouldEqual ((db4.parentScopeID, db4.id))
        ids3Map(db5.scopeID) shouldEqual ((db5.parentScopeID, db5.id))
        ids3Map(db2.globalID) shouldEqual ((db2.parentScopeID, db2.id))
        ids3Map(db4.globalID) shouldEqual ((db4.parentScopeID, db4.id))
        ids3Map(db5.globalID) shouldEqual ((db5.parentScopeID, db5.id))
      }

      "reflects modified dbs" in {
        val db1 = newDB(ScopeID.RootID)
        val dbKey = (db1.parentScopeID, db1.id)

        // have to do some munging to get the right state via WriteAdaptor. We
        // want a db with an updated global ID. This should in theory never
        // happen but we saw these in preview.
        val data =
          (ctx ! ModelStore.get(ScopeID.RootID, db1.id.toDocID)).get.data
        val data0 = data.update(Database.GlobalIDField -> GlobalDatabaseID(123))

        val ec = EvalContext.write(RootAuth, Timestamp.MaxMicros, APIVersion.Default)
        ctx ! DatabaseWriteConfig.Default.write(
          ec,
          db1.id.toDocID,
          data0,
          false,
          RootPosition)

        val ids =
          ctx ! Database.getLiveGlobalIDsInHierarchy(db1.parentScopeID, db1.id)
        val idsMap = ids.toMap

        ids.size shouldEqual 3
        idsMap(db1.scopeID) shouldEqual dbKey
        idsMap(db1.globalID) shouldEqual dbKey
        idsMap(GlobalDatabaseID(123)) shouldEqual dbKey

        // Assert that LookupStore agrees w/ us. There are two live global IDs
        // because Lookups does not handle updates correctly.
        (ctx ! Store.getLatestDatabaseID(db1.scopeID)) shouldEqual Some(dbKey)
        (ctx ! Store.getLatestDatabaseID(db1.globalID)) shouldEqual Some(dbKey)
        (ctx ! Store.getLatestDatabaseID(GlobalDatabaseID(123))) shouldEqual Some(
          dbKey)
      }
    }

    "restored database does not result in multiple live lookups" in {
      import fauna.repo.store.CollectionAtTS

      // create a parent, child, gchild
      val scope1 = ctx ! newScope(RootAuth)
      val db1 = (ctx ! Database.getUncached(scope1)).get
      val scope2 = ctx ! newScope(Auth.adminForScope(scope1))
      val db2 = (ctx ! Database.getUncached(scope2)).get
      val scope3 = ctx ! newScope(Auth.adminForScope(scope2))
      val db3 = (ctx ! Database.getUncached(scope3)).get
      val dbs = Seq(db1, db2, db3)
      val globalIDs = dbs.map { _.globalID }

      val getLookupsQ = globalIDs
        .map { globalID =>
          val l = Store.lookups(globalID).mapValuesT { (globalID, _) }
          val coll = CollectionAtTS(l, Timestamp.MaxMicros, 0.seconds)
          coll.flattenT.map { _.filter { _.isCreate } }
        }
        .sequence
        .map { _.flatten }

      // verify that current live lookups point to our live db tree
      val lookups1 = (ctx ! getLookupsQ).map { l => (l.scope, l.id) }.toSet
      val dbs1 = dbs.map { db => (db.parentScopeID, db.id.toDocID) }.toSet
      lookups1 shouldEqual dbs1

      // fake snapshot import by recreating the above in a restore tmp
      val tmpdb = (ctx ! Database.getUncached(ctx ! newScope(RootAuth))).get
      val scope10 = ScopeID(ctx.nextID())
      val scope20 = ScopeID(ctx.nextID())
      val scope30 = ScopeID(ctx.nextID())
      val ss = Map(
        ScopeID.RootID -> tmpdb.scopeID,
        db1.scopeID -> scope10,
        db2.scopeID -> scope20,
        db3.scopeID -> scope30)

      val versions = dbs flatMap { db =>
        ctx ! ModelStore.versions(db.parentScopeID, db.id.toDocID).flattenT
      }
      versions foreach { v =>
        val newScope = ss(v.data(Database.ScopeField))
        val globalID = v.data(Database.GlobalIDField)
        val data =
          v.data.update(Database.ScopeField -> newScope)
        val q1 =
          Query.write(
            VersionAdd(
              ss(v.parentScopeID),
              v.id,
              v.ts,
              v.action,
              v.schemaVersion,
              data,
              None))

        val q2 = LookupStore.add(
          LiveLookup(
            newScope,
            ss(v.parentScopeID),
            v.id,
            v.ts,
            v.action.toSetAction),
          false)
        val q3 = LookupStore.add(
          LiveLookup(
            globalID,
            ss(v.parentScopeID),
            v.id,
            v.ts,
            v.action.toSetAction),
          false)

        ctx ! Seq(q1, q2, q3).join
      }

      // verify our recreated db tree and the original have matching globalIDs
      val db10 = (ctx ! Database.forScope(scope10)).get
      db10.globalID shouldEqual db1.globalID
      val db20 = (ctx ! Database.forScope(scope20)).get
      db20.globalID shouldEqual db2.globalID
      val db30 = (ctx ! Database.forScope(scope30)).get
      db30.globalID shouldEqual db3.globalID

      // "restore" our copy to the original. db1 and descendants will be deleted
      val moveQ =
        Database.moveDatabase(db10, db1, restore = true, RootPosition)
      (ctx ! moveQ).isRight shouldEqual true

      val lookups2 = (ctx ! getLookupsQ).map { l => (l.scope, l.id) }.toSet
      val db100 = (ctx ! Database.forScope(scope10)).get
      val dbs2 = Seq(db100, db20, db30).map { db =>
        (db.parentScopeID, db.id.toDocID)
      }.toSet

      // our restored db will be in the root scope
      db100.parentScopeID shouldEqual ScopeID.RootID
      lookups2 shouldEqual dbs2
    }

    "enable/disable databases" in {
      val scope = ctx ! newScope(RootAuth)

      val dbID = (ctx ! Database.forScope(scope)).get.id
      val ec = EvalContext.write(RootAuth, Timestamp.MaxMicros, APIVersion.Default)

      // database is by default enabled
      ctx ! Database.isDisabled(scope) shouldBe false

      // try to enable an enabled database has no effect
      ctx ! DatabaseWriteConfig.Default.enable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe false

      // disable database
      ctx ! DatabaseWriteConfig.Default.disable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe true

      // try to disable a disabled database has no effect either
      ctx ! DatabaseWriteConfig.Default.disable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe true
    }

    "enabled/disable child databases" in {
      val scope = ctx ! newScope(RootAuth)
      val childScope = ctx ! newScope(Auth.adminForScope(scope))

      val dbID = (ctx ! Database.forScope(scope)).get.id
      val ec = EvalContext.write(RootAuth, Timestamp.MaxMicros, APIVersion.Default)

      // database is by default enabled
      ctx ! Database.isDisabled(scope) shouldBe false
      ctx ! Database.isDisabled(childScope) shouldBe false

      // try to enable an enabled database has no effect
      ctx ! DatabaseWriteConfig.Default.enable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe false
      ctx ! Database.isDisabled(childScope) shouldBe false

      // disable database
      ctx ! DatabaseWriteConfig.Default.disable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe true
      ctx ! Database.isDisabled(childScope) shouldBe true

      // try to disable a disabled database has no effect either
      ctx ! DatabaseWriteConfig.Default.disable(ec, dbID.toDocID)
      ctx ! Database.isDisabled(scope) shouldBe true
      ctx ! Database.isDisabled(childScope) shouldBe true
    }

    "move database" in {
      // moves <db> from <dbA> to <dbB>
      val scopeA = ctx ! newScope(RootAuth)
      val scopeB = ctx ! newScope(RootAuth)

      val scope = ctx ! newScope(Auth.adminForScope(scopeA))
      val db = (ctx ! Database.forScope(scope)).get
      val (secret, _) = ctx ! mkKey(db.name, "server", Auth.adminForScope(scopeA))

      val beforeAuth = (ctx ! Auth.lookup(secret)).get

      beforeAuth.source match {
        case KeyLogin(key) => key.parentScopeID should equal(scopeA)
        case s             => fail(s"unexpected source $s")
      }

      ctx ! mkCollection(beforeAuth, MkObject("name" -> "before_move"))

      (ctx ! Collection.idByNameActive(db.scopeID, "before_move")).isEmpty should be(
        false)

      val dbA = (ctx ! Database.forScope(scopeA)).get
      val dbB = (ctx ! Database.forScope(scopeB)).get

      (ctx ! runQuery(
        Auth.adminForScope(dbA.scopeID),
        Exists(DatabaseRef(db.name)))) shouldBe TrueL
      (ctx ! runQuery(
        Auth.adminForScope(dbB.scopeID),
        Exists(DatabaseRef(db.name)))) shouldBe FalseL

      (ctx ! Database.moveDatabase(
        db,
        dbB,
        restore = false,
        RootPosition)).isRight shouldBe true

      (ctx ! runQuery(
        Auth.adminForScope(dbA.scopeID),
        Exists(DatabaseRef(db.name)))) shouldBe FalseL
      (ctx ! runQuery(
        Auth.adminForScope(dbB.scopeID),
        Exists(DatabaseRef(db.name)))) shouldBe TrueL

      val afterAuth = (ctx ! Auth.lookup(secret)).get

      afterAuth.source match {
        case KeyLogin(key) => key.parentScopeID should equal(scopeB)
        case s             => fail(s"unexpected source $s")
      }

      ctx ! mkCollection(afterAuth, MkObject("name" -> "after_move"))

      (ctx ! Collection.idByNameActive(db.scopeID, "before_move")).isEmpty should be(
        false)
      (ctx ! Collection.idByNameActive(db.scopeID, "after_move")).isEmpty should be(
        false)
    }

    "restore in place" in {
      // moves database <db> from <tmp> to <root>
      val root = ctx ! newScope(RootAuth)
      val tmp = ctx ! newScope(RootAuth)

      val db = ctx ! newScope(Auth.adminForScope(root))
      def to = (ctx ! Database.forScope(db)).get

      val rewrittenScope = ScopeID(10)
      def from = (ctx ! Database.forScope(rewrittenScope)).get

      val dbName = to.name
      val dbID = to.id.toDocID

      val (_, key) = ctx ! mkKey(dbName, "server", Auth.adminForScope(root))

      val dbVersion =
        ctx ! runQuery(Auth.adminForScope(root), Get(DatabaseRef(dbName))) match {
          case VersionL(v, _) => v
          case _              => fail("fail")
        }

      val keyVersion = ctx ! Store.getUnmigrated(root, key.id.toDocID)

      val globalID = dbVersion.data(Database.GlobalIDField)

      (ctx ! Database.forGlobalID(globalID)).get shouldBe to

      // creates a clone of the <db> with a new scope under <tmp>
      val newData = dbVersion.data.update(Database.ScopeField -> rewrittenScope)
      val writeTS = dbVersion.ts.resolve(dbVersion.ts.validTS)
      ctx ! Query.write(
        VersionAdd(
          tmp,
          dbID,
          writeTS,
          dbVersion.action,
          SchemaVersion.Min,
          newData,
          None))
      ctx ! LookupStore.add(
        LiveLookup(rewrittenScope, tmp, dbID, writeTS, dbVersion.action.toSetAction),
        false)
      ctx ! LookupStore.add(
        LiveLookup(globalID, tmp, dbID, writeTS, dbVersion.action.toSetAction),
        false)

      ctx ! PublicCollection.Key(tmp).insert(key.id, keyVersion.get.data)

      from.parentScopeID shouldBe tmp
      (ctx ! Database.forGlobalID(globalID)).get shouldBe to

      // Keys added between the snapshot and restore should be gone after the move.
      val (_, deadKey) = ctx ! mkKey(dbName, "admin", Auth.adminForScope(root))
      val beforeKeys = ctx ! (Key.forDatabase(root, from.id) mapValuesT { k =>
        (k.id, k.globalID, k.scopeID)
      } flattenT)

      val expected = Vector(key, deadKey) map { k => (k.id, k.globalID, k.scopeID) }
      beforeKeys should contain only (expected: _*)

      // <from> & <to> are essentially the same databases, but located on different
      // parents
      (ctx ! Database.moveDatabase(
        from,
        to,
        restore = true,
        RootPosition)).isRight shouldBe true

      from.parentScopeID shouldBe root
      (ctx ! Database.forGlobalID(globalID)).get shouldBe from

      // get by name should return the new database
      ctx ! runQuery(Auth.adminForScope(root), Get(DatabaseRef(dbName))) match {
        case VersionL(v, _) =>
          v.id shouldBe from.id.toDocID // shouldn't this id be different?
          v.parentScopeID shouldBe root
        case _ =>
          fail("fail")
      }

      val newKey = (ctx ! Key.forDatabase(root, from.id).headValueT).get

      val afterKeys = ctx ! (Key.forDatabase(root, from.id) mapValuesT { k =>
        (k.id, k.globalID, k.scopeID)
      } flattenT)

      afterKeys should contain only ((newKey.id, newKey.globalID, rewrittenScope))

      an[Exception] shouldBe thrownBy {
        to
      }
    }

    "fail to restore in place if not forced" in {
      // moves database <db> from <tmp> to <root>
      val root = ctx ! newScope(RootAuth)
      val tmp = ctx ! newScope(RootAuth)

      val db = ctx ! newScope(Auth.adminForScope(root))
      def to = (ctx ! Database.forScope(db)).get

      val rewrittenScope = ScopeID(10)
      def from = (ctx ! Database.forScope(rewrittenScope)).get

      val dbName = to.name
      val dbID = to.id.toDocID

      val version =
        ctx ! runQuery(Auth.adminForScope(root), Get(DatabaseRef(dbName))) match {
          case VersionL(v, _) => v
          case _              => fail("fail")
        }

      val globalID = version.data(Database.GlobalIDField)

      (ctx ! Database.forGlobalID(globalID)).get shouldBe to

      // creates a clone of the <db> with a new scope under <tmp>
      val newData = version.data.update(Database.ScopeField -> rewrittenScope)
      val writeTS = version.ts.resolve(version.ts.validTS)
      ctx ! Query.write(
        VersionAdd(
          tmp,
          dbID,
          writeTS,
          version.action,
          SchemaVersion.Min,
          newData,
          None))
      ctx ! LookupStore.add(
        LiveLookup(rewrittenScope, tmp, dbID, writeTS, version.action.toSetAction),
        false)
      ctx ! LookupStore.add(
        LiveLookup(globalID, tmp, dbID, writeTS, version.action.toSetAction),
        false)

      from.parentScopeID shouldBe tmp
      (ctx ! Database.forGlobalID(globalID)).get shouldBe to

      // <from> & <to> are essentially the same databases, but located on different
      // parents
      (ctx ! Database.moveDatabase(
        from,
        to,
        restore = false,
        RootPosition)).isLeft shouldBe true
    }

    "cleanup lookup entries after move database even when versions disagree" in {
      val parentScopeID = ctx ! newScope(RootAuth)
      val databaseID = DatabaseID(408469308055748696L)
      val scopeID = ScopeID(408465413388633176L)
      val globalID = GlobalDatabaseID(408465413388632152L)

      val APIVersionField = Field[IRValue]("api_version")
      val GlobalIDField = Field[IRValue]("global_id")
      val TSField = Field[Long]("ts")

      ctx ! Query.write(
        VersionAdd(
          parentScopeID,
          databaseID.toDocID,
          Resolved(
            Timestamp.parse("2016-03-24T19:07:46.490Z"),
            Timestamp.parse("6043-05-07T15:20:25.442305Z")),
          DocAction.Create,
          SchemaVersion.Min,
          Data(
            SchemaNames.NameField -> SchemaNames.Name("db"),
            Database.ScopeField -> scopeID),
          None
        ))

      ctx ! Query.write(
        VersionAdd(
          parentScopeID,
          databaseID.toDocID,
          Resolved(Timestamp.parse("2019-07-16T18:31:59.240Z")),
          DocAction.Update,
          SchemaVersion.Min,
          Data(
            SchemaNames.NameField -> SchemaNames.Name("db"),
            Database.ScopeField -> scopeID,
            APIVersionField -> "2.0"),
          Some(Diff(APIVersionField -> NullV, TSField -> 1458846466490000L))
        ))

      ctx ! Query.write(
        VersionAdd(
          parentScopeID,
          databaseID.toDocID,
          Resolved(Timestamp.parse("2021-12-03T00:00:47.983Z")),
          DocAction.Update,
          SchemaVersion.Min,
          Data(
            SchemaNames.NameField -> SchemaNames.Name("db"),
            Database.ScopeField -> scopeID,
            APIVersionField -> "2.0",
            Database.GlobalIDField -> globalID),
          Some(Diff(GlobalIDField -> NullV, TSField -> 1563301919240000L))
        ))

      val globalEntry =
        LiveLookup(
          globalID,
          parentScopeID,
          databaseID.toDocID,
          Resolved(
            Timestamp.parse("2016-03-24T19:07:46.490Z"),
            Timestamp.parse("6043-05-07T15:20:25.442305Z")),
          DocAction.Create.toSetAction
        )

      val scopeEntry =
        LiveLookup(
          scopeID,
          parentScopeID,
          databaseID.toDocID,
          Resolved(
            Timestamp.parse("2016-03-24T19:07:46.490Z"),
            Timestamp.parse("6043-05-07T15:20:25.442305Z")),
          DocAction.Create.toSetAction
        )

      ctx ! LookupStore.add(globalEntry)
      ctx ! LookupStore.add(scopeEntry)

      (ctx ! SchemaCollection
        .Database(parentScopeID)
        .versions(databaseID)
        .flattenT).size shouldBe 3

      (ctx ! Store.lookups(globalID).flattenT) shouldBe Seq(globalEntry)
      (ctx ! Store.lookups(scopeID).flattenT) shouldBe Seq(scopeEntry)

      val dstID = ctx ! newScope(RootAuth)

      val Right(VersionL(version, _)) = ctx ! Database.moveDatabase(
        target = (ctx ! Database.forScope(scopeID)).get,
        destination = (ctx ! Database.forScope(dstID)).get,
        restore = false,
        pos = RootPosition)

      // check if database document is gone
      (ctx ! SchemaCollection
        .Database(parentScopeID)
        .versions(databaseID)
        .flattenT).size shouldBe 0

      val globalEntries = ctx ! Store.lookups(globalID).flattenT
      val scopeEntries = ctx ! Store.lookups(scopeID).flattenT

      // assert only 1 lookup entries exist for scope/global id
      globalEntries.size shouldBe 1
      scopeEntries.size shouldBe 1

      // and it's different from the previous
      globalEntries.head should not be globalEntry
      scopeEntries.head should not be scopeEntry

      // check if the new lookup entries point to the new document
      globalEntries.head.scope shouldBe version.parentScopeID
      globalEntries.head.id shouldBe version.docID

      scopeEntries.head.scope shouldBe version.parentScopeID
      scopeEntries.head.id shouldBe version.docID
    }
  }
}
