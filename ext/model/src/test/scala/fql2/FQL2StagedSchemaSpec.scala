package fauna.model.test

import fauna.atoms._
import fauna.auth.{ AdminPermissions, Auth }
import fauna.lang.syntax._
import fauna.model.{ AccessProvider, Collection, Index }
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.schema.{
  Result,
  SchemaCollection,
  SchemaError,
  SchemaIndexStatus,
  SchemaManager,
  SchemaSource,
  SchemaStatus
}
import fauna.model.schema.index.CollectionIndexManager
import fauna.model.tasks.{ Deleted, Live, RetentionPolicy, TaskExecutor }
import fauna.model.Task
import fauna.repo.cassandra.CassandraService
import fauna.repo.query.Query
import fauna.repo.store.CacheStore
import fauna.repo.values.Value
import fauna.repo.Store
import fauna.storage.doc.ConcretePath
import fauna.storage.index.IndexTerm
import fauna.storage.ir._
import fql.color.ColorKind
import fql.schema.DiffRender
import org.scalactic.source.Position
import scala.concurrent.duration._

class FQL2StagedSchemaBaseSpec extends FQL2Spec {

  var auth: Auth = _

  val exec = TaskExecutor(ctx)
  def tasks =
    ctx ! Task.getRunnableByHost(CassandraService.instance.localID.get).flattenT

  before {
    auth = newDB.withPermissions(AdminPermissions)
  }

  def pin(): SchemaVersion = {
    ctx ! SchemaStatus.pin(new FQLInterpreter(auth))
  }

  def commit(): Unit = {
    ctx ! SchemaStatus.commit(new FQLInterpreter(auth)) match {
      case Result.Ok(_) =>
        ctx ! CacheStore.invalidateScope(auth.scopeID)
      case Result.Err(e) => fail(s"Commit failed.\n${e}")
    }
  }

  def commitErr(): List[SchemaError] = {
    ctx ! SchemaStatus.commit(new FQLInterpreter(auth)) match {
      case Result.Ok(_)  => fail("expected commit to fail, but it passed")
      case Result.Err(e) => e
    }
  }

  def abandon(): Unit = {
    ctx ! SchemaStatus.abandon(new FQLInterpreter(auth))
  }

  def status(): Option[SchemaIndexStatus] = {
    ctx ! CacheStore.invalidateScope(auth.scopeID)
    ctx ! SchemaIndexStatus.forScope(auth.scopeID)
  }

  def diff(): String = {
    ctx ! (for {
      diff <- SchemaManager
        .stagedDiff(auth.scopeID)
        .map(_.getOrElse(sys.error("staged schema is not valid")))
      before <- SchemaSource.activeFSLFiles(auth.scopeID)
      after  <- SchemaSource.stagedFSLFiles(auth.scopeID)
    } yield {
      val beforeSrc = before.view.map { file => file.src -> file.content }
      val afterSrc = after.view.map { file => file.src -> file.content }
      DiffRender.renderSemantic(
        beforeSrc.toMap,
        afterSrc.toMap,
        diff.diffs,
        ColorKind.None)
    })
  }

  def statusStr(): String = {
    ctx ! (for {
      summary <- SchemaManager
        .stagedDiff(auth.scopeID)
        .map(_.getOrElse(sys.error("staged schema is not valid")))
    } yield {
      summary.statusStr
    })
  }

  def pendingSummary(): String = {
    ctx ! (for {
      summary <- SchemaManager
        .stagedDiff(auth.scopeID)
        .map(_.getOrElse(sys.error("staged schema is not valid")))
    } yield {
      summary.pendingSummary
    })
  }

  def checkSortedIndex(index: Index, terms: Vector[IndexTerm], expected: Seq[DocID])(
    implicit pos: Position): Unit = {
    ctx ! Store
      .sortedIndex(index, terms)
      .flattenT
      .mapT(_.docID) shouldBe expected
  }
}

class FQL2StagedSchemaSpec extends FQL2StagedSchemaBaseSpec {
  "stores staged items for updates" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|function foo() {
           |  1
           |}""".stripMargin
    )

    val pinned = pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|function foo() {
           |  2
           |}""".stripMargin
    )

    val latest = ctx ! CacheStore.getLastSeenSchemaUncached(auth.scopeID).map(_.get)
    val active0 =
      ctx ! SchemaStatus.forScope(auth.scopeID).map(_.activeSchemaVersion.get)

    active0 shouldBe pinned
    latest should be > pinned

    // Functions should use the pinned version.
    evalOk(auth, "foo()") shouldEqual Value.Int(1)

    commit()

    // There should no longer be an active schema version set
    eventually(timeout(5.seconds)) {
      val active1 =
        ctx ! SchemaStatus.forScope(auth.scopeID).map(_.activeSchemaVersion)
      active1 shouldBe None
    }

    // Functions should use the latest version after a commit.
    evalOk(auth, "foo()") shouldEqual Value.Int(2)
  }

  "disallow staging an invalid update" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |function foo() {
           |  User.create({ name: 'Bob' })
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'Alice' })")

    pin()

    // Staged updates should be entirely valid. If the function `foo` were checked
    // against the _active_ schema version, this update would pass (which it
    // shouldn't).
    updateSchemaErr(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  migrations {
           |    add .email
           |    backfill .email = "foo@example.com"
           |  }
           |}
           |
           |function foo() {
           |  User.create({ name: 'Bob' })
           |}""".stripMargin
    ) shouldBe (
      """|error: Type `{ name: "Bob" }` does not have field `email`
         |at main.fsl:12:15
         |   |
         |12 |   User.create({ name: 'Bob' })
         |   |               ^^^^^^^^^^^^^^^
         |   |""".stripMargin
    )
  }

  "shows active items for live queries" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |function foo() {
           |  User.create({ name: 'Carol' })
           |}""".stripMargin
    )

    val doc1 = evalOk(auth, "User.create({ name: 'Alice' })").as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  first_name: String
           |  email: String
           |
           |  migrations {
           |    add .email
           |    backfill .email = "foo@example.com"
           |
           |    move .name -> .first_name
           |  }
           |}
           |
           |function foo() {
           |  User.create({ first_name: 'Other', email: 'bar@example.com' })
           |}""".stripMargin
    )

    val doc2 = evalOk(auth, "User.create({ name: 'Bob' })").as[DocID]
    // Old versions of UDfs should be executed.
    val doc3 = evalOk(auth, "foo()").as[DocID]

    // New versions should use the active schema version (min in this case, because
    // we started with no migrations), not the staged one.
    (ctx ! ModelStore.get(
      auth.scopeID,
      doc1)).get.schemaVersion shouldBe SchemaVersion.Min
    (ctx ! ModelStore.get(
      auth.scopeID,
      doc2)).get.schemaVersion shouldBe SchemaVersion.Min
    (ctx ! ModelStore.get(
      auth.scopeID,
      doc3)).get.schemaVersion shouldBe SchemaVersion.Min

    // Docs should not get the staged field, or see renames.
    evalOk(
      auth,
      "User.all().map(d => { let d: Any = d; d.data }).toArray()") shouldBe (
      Value.Array(
        Value.Struct("name" -> Value.Str("Alice")),
        Value.Struct("name" -> Value.Str("Bob")),
        Value.Struct("name" -> Value.Str("Carol")))
    )
  }

  "should write to staged indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val alice =
      evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")
        .as[DocID]
    val bob = evalOk(auth, "User.create({ name: 'Bob', email: 'bob@example.com' })")
      .as[DocID]
    val carol =
      evalOk(auth, "User.create({ name: 'Carol', email: 'carol@example.com' })")
        .as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.byName", typecheck = false).isNull shouldBe false
    evalOk(auth, "User.byEmail", typecheck = false).isNull shouldBe true

    val coll =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll.active.get.collIndexes.map(_.indexID) shouldBe List(IndexID(32768))
    coll.staged.get.collIndexes.map(_.indexID) shouldBe List(
      IndexID(32768),
      IndexID(32769))

    val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)
    val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)

    checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
    checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
    checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

    checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
    checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
    checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))
  }

  "staged indexes should use the pending migrations" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val alice =
      evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")
        .as[DocID]
    val bob = evalOk(auth, "User.create({ name: 'Bob', email: 'bob@example.com' })")
      .as[DocID]
    val carol =
      evalOk(auth, "User.create({ name: 'Carol', email: 'carol@example.com' })")
        .as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  new_email: String
           |
           |  migrations {
           |    move .email -> .new_email
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.new_email]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.byName", typecheck = false).isNull shouldBe false
    evalOk(auth, "User.byEmail", typecheck = false).isNull shouldBe true

    val coll =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll.active.get.collIndexes.map(_.indexID) shouldBe List(IndexID(32768))
    coll.staged.get.collIndexes.map(_.indexID) shouldBe List(
      IndexID(32768),
      IndexID(32769))

    val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)
    val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)

    checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
    checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
    checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

    checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
    checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
    checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))
  }

  "live writes should use staged schema for staged indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val alice =
      evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")
        .as[DocID]
    val bob = evalOk(auth, "User.create({ name: 'Bob', email: 'bob@example.com' })")
      .as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  new_email: String
           |
           |  migrations {
           |    move .email -> .new_email
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.new_email]
           |  }
           |}""".stripMargin
    )

    val carol =
      evalOk(auth, "User.create({ name: 'Carol', email: 'carol@example.com' })")
        .as[DocID]

    val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)
    val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)

    checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
    checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
    checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

    checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
    checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
    checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))
  }

  "indexes should remain in their old state when used by both schemas" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    val alice =
      evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")
        .as[DocID]
    val bob = evalOk(auth, "User.create({ name: 'Bob', email: 'bob@example.com' })")
      .as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  new_email: String
           |
           |  migrations {
           |    move .email -> .new_email
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.new_email]
           |  }
           |}""".stripMargin
    )

    // This should go through the `StagedIndexer` path for `byEmail`.
    val carol =
      evalOk(auth, "User.create({ name: 'Carol', email: 'carol@example.com' })")
        .as[DocID]

    {
      val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)
      val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)

      // The index's definition should remain unchanged.
      val byEmailDoc =
        ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail.id).map(_.get)
      byEmailDoc.data.get(ConcretePath("terms")) shouldBe Some(
        ArrayV(MapV("field" -> ArrayV("email"), "mva" -> false)))

      checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
      checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
      checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

      checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
      checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
      checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))
    }

    commit()

    // This should go through the normal indexer path for `byEmail`, now that the
    // schema has been comitted.
    val dylan =
      evalOk(auth, "User.create({ name: 'Dylan', new_email: 'dylan@example.com' })")
        .as[DocID]

    {
      val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)
      val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)

      // The index's definition should now be updated.
      val byEmailDoc =
        ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail.id).map(_.get)
      byEmailDoc.data.get(ConcretePath("terms")) shouldBe Some(
        ArrayV(MapV("field" -> ArrayV("new_email"), "mva" -> false)))

      checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
      checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
      checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))
      checkSortedIndex(byName, Vector(IndexTerm("Dylan")), Seq(dylan))

      checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
      checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
      checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))
      checkSortedIndex(byEmail, Vector(IndexTerm("dylan@example.com")), Seq(dylan))
    }
  }

  "should revert previous staged schema changes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      "User.create({ name: 'Alice', email: 'alice@example.com', phone: '12345' })")

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    val coll0 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    val byName = IndexID(32768)
    val byEmail = IndexID(32769)
    val byPhone = IndexID(32770)

    coll0.staged.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)

    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail).map(_.get)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byPhone {
           |    terms [.phone]
           |  }
           |}""".stripMargin
    )

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(byName, byPhone)

    // byEmail should be deleted, and byName and byPhone should be live.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail) shouldBe empty
    ctx ! SchemaCollection.Index(auth.scopeID).get(byPhone).map(_.get)
  }

  "should revert previous migrations" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val alice =
      evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")
        .as[DocID]
    val bob = evalOk(auth, "User.create({ name: 'Bob', email: 'bob@example.com' })")
      .as[DocID]
    val carol =
      evalOk(auth, "User.create({ name: 'Carol', email: 'carol@example.com' })")
        .as[DocID]

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email_1: String
           |
           |  migrations {
           |    move .email -> .email_1
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email_1]
           |  }
           |}""".stripMargin
    )

    val byName = ctx ! Index.getUncached(auth.scopeID, IndexID(32768)).map(_.get)
    val byEmail = ctx ! Index.getUncached(auth.scopeID, IndexID(32769)).map(_.get)

    checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
    checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
    checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

    checkSortedIndex(byEmail, Vector(IndexTerm("alice@example.com")), Seq(alice))
    checkSortedIndex(byEmail, Vector(IndexTerm("bob@example.com")), Seq(bob))
    checkSortedIndex(byEmail, Vector(IndexTerm("carol@example.com")), Seq(carol))

    // Note that these migrations are relative to the active schema, not the previous
    // staged schema.
    //
    // Also, we can't just use rename, as that would re-use the previous index.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email_2: String
           |
           |  migrations {
           |    split .email -> .email_2, .tmp
           |    backfill .email_2 = ""
           |    drop .tmp
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email_2]
           |  }
           |}""".stripMargin
    )

    val byEmail2 = ctx ! Index.getUncached(auth.scopeID, IndexID(32770)).map(_.get)

    checkSortedIndex(byName, Vector(IndexTerm("Alice")), Seq(alice))
    checkSortedIndex(byName, Vector(IndexTerm("Bob")), Seq(bob))
    checkSortedIndex(byName, Vector(IndexTerm("Carol")), Seq(carol))

    checkSortedIndex(byEmail2, Vector(IndexTerm("alice@example.com")), Seq(alice))
    checkSortedIndex(byEmail2, Vector(IndexTerm("bob@example.com")), Seq(bob))
    checkSortedIndex(byEmail2, Vector(IndexTerm("carol@example.com")), Seq(carol))
  }

  "should stage an index being deleted" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      "User.create({ name: 'Alice', email: 'alice@example.com', phone: '12345' })")

    pin()

    val coll0 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    val byName = IndexID(32769)
    val byEmail = IndexID(32768)

    coll0.active.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)
    coll0.staged.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll1.active.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)
    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(byName)

    // Both indexes should still be live, as they are used by the active schema.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail).map(_.get)

    commit()

    // The deleted index should be deleted now.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail) shouldBe empty
  }

  "should retain a collection staged for deletion so it isn't GC'd" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
            |  name: String
            |}
            |
            |collection Loser {
            |  name: String
            |}""".stripMargin
    )

    pin()

    val user = CollectionID(1024)
    val loser = CollectionID(1025)

    retentionOf(user) shouldBe Live
    retentionOf(loser) shouldBe Live

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
             |  name: String
             |}""".stripMargin
    )

    // Computing retention at the snapshot time is like having no grace period for
    // GC.
    def retentionOf(id: CollectionID) = ctx ! Query.snapshotTime.flatMap { snap =>
      RetentionPolicy.Default.isCollectionRetained(auth.scopeID, id, snap)
    }

    retentionOf(user) shouldBe Live
    retentionOf(loser) shouldBe Live

    commit()

    retentionOf(user) shouldBe Live
    retentionOf(loser) shouldBe Deleted
  }

  "should abandon a schema change" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'Alice' })")

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  migrations {
           |    add .email
           |    backfill .email = ""
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    val byName = IndexID(32768)
    val byEmail = IndexID(32769)

    val coll0 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll0.active.get.collIndexes.map(_.indexID) shouldBe List(byName)
    coll0.staged.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)

    // Both indexes should be live, as they are used by the staged schema.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail).map(_.get)

    abandon()

    // The active schema version should be unset.
    eventually(timeout(5.seconds)) {
      ctx ! SchemaStatus
        .forScope(auth.scopeID)
        .map(_.activeSchemaVersion) shouldBe None
    }

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    // The staged schema should no longer exist.
    coll1.active.get.collIndexes.map(_.indexID) shouldBe List(byName)
    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(byName)

    // `byEmail` should be deleted now
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail) shouldBe empty

    // And the schema source should be reverted.
    schemaContent(auth, "main.fsl") shouldBe Some(
      """|collection User {
         |  name: String
         |
         |  index byName {
         |    terms [.name]
         |  }
         |}""".stripMargin
    )
  }

  "abandon should revert deleted files" in {
    // Note that we can't actually schema items in a staged push, as that would break
    // name lookups for active queries. So, I'll test re-creating docs by just making
    // an empty file.
    updateSchemaOk(
      auth,
      "foo.fsl" -> "function foo() { 0 }",
      "bar.fsl" -> "// I contain nothing!"
    )

    pin()

    updateSchemaOk(auth, "foo.fsl" -> "function foo() { 0 }")

    abandon()

    schemaContent(auth, "foo.fsl") shouldBe Some("function foo() { 0 }")
    schemaContent(auth, "bar.fsl") shouldBe Some("// I contain nothing!")
  }

  "abandon should revert created files" in {
    updateSchemaOk(
      auth,
      "foo.fsl" -> "function foo() { 0 }"
    )

    pin()

    updateSchemaOk(
      auth,
      "foo.fsl" -> "function foo() { 0 }",
      "bar.fsl" -> "function bar() { 0 }")

    abandon()

    schemaContent(auth, "foo.fsl") shouldBe Some("function foo() { 0 }")
    schemaContent(auth, "bar.fsl") shouldBe None
  }

  "should abandon an index being deleted" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    evalOk(
      auth,
      "User.create({ name: 'Alice', email: 'alice@example.com', phone: '12345' })")

    pin()

    val coll0 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    val byName = IndexID(32769)
    val byEmail = IndexID(32768)

    coll0.active.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)
    coll0.staged.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |  phone: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll1.active.get.collIndexes.map(_.indexID) shouldBe List(byName, byEmail)
    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(byName)

    // Both indexes should still be live, as they are used by the active schema.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail).map(_.get)

    abandon()

    // The staged changes should be reverted, and these indexes should be left alone.
    ctx ! SchemaCollection.Index(auth.scopeID).get(byName).map(_.get)
    ctx ! SchemaCollection.Index(auth.scopeID).get(byEmail).map(_.get)
  }

  "should abandon a function being modified" in {
    updateSchemaOk(auth, "main.fsl" -> "function foo() { 1 }")

    pin()

    updateSchemaOk(auth, "main.fsl" -> "function foo() { 2 }")

    // `foo` should still be the active version.
    evalOk(auth, "if (foo() != 1) abort(foo())")

    abandon()

    // `foo` should still be the old version, because we abandoned the staged change.
    evalOk(auth, "if (foo() != 1) abort(foo())")
  }

  "should keep index statuses for indexes that built during a staged schema" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 129).forEach(i => {
         |  User.create({ name: "User #{i}", email: "" })
         |})""".stripMargin)

    // Trigger an index build.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    pin() // Pin an empty schema update.

    // The index should still be building.
    evalOk(
      auth,
      "if (Collection.byName('User')?.indexes.byName?.status != 'building') abort(0)")

    // Finish building the index.
    while (tasks.nonEmpty) exec.step()

    // The index should be ready.
    evalOk(
      auth,
      "if (Collection.byName('User')?.indexes.byName?.status != 'complete') abort(0)")

    // Now abandon, and the index should still be ready.
    abandon()

    evalOk(
      auth,
      "if (Collection.byName('User')?.indexes.byName?.status != 'complete') abort(0)")
  }

  "can derive a staged schema status after indexes finish building" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 129).forEach(i => {
         |  User.create({ name: "user-#{i}" })
         |})""".stripMargin)

    status() shouldBe None

    pin()

    status() shouldBe Some(SchemaIndexStatus.Ready)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
             |  name: String
             |
             |  index byName {
             |    terms [.name]
             |  }
             |}""".stripMargin
    )

    status() shouldBe Some(SchemaIndexStatus.Pending)
    tasks.isEmpty shouldBe false

    // Finish building the index.
    while (tasks.nonEmpty) exec.step()

    status() shouldBe Some(SchemaIndexStatus.Ready)

    commit()
    status() shouldBe None
  }

  "can derive a staged schema status after abandoning a change" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(
      auth,
      """|Set.sequence(0, 129).forEach(i => {
         |  User.create({ name: "user-#{i}" })
         |})""".stripMargin)

    status() shouldBe None

    pin()

    status() shouldBe Some(SchemaIndexStatus.Ready)

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    status() shouldBe Some(SchemaIndexStatus.Pending)
    tasks.isEmpty shouldBe false

    // Abandon during an index build, and the status should immediately go away.
    abandon()
    status() shouldBe None
  }

  "pushing a second staged schema should not rebuild everything" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |}""".stripMargin
    )

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    val coll =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll.active.get.collIndexes.map(_.indexID) shouldBe Nil
    coll.staged.get.collIndexes.map(_.indexID) shouldBe List(IndexID(32768))

    // This update keeps the `byName` index around, even though the schema is
    // entirely replaced.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll1.active.get.collIndexes.map(_.indexID) shouldBe Nil
    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(
      IndexID(32768),
      IndexID(32769))
  }

  "pushing a second staged schema should handle renames correctly" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'Alice', email: 'alice@example.com' })")

    pin()

    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name_1: String
           |  email: String
           |
           |  migrations {
           |    move .name -> .name_1
           |  }
           |
           |  index byName {
           |    terms [.name_1]
           |  }
           |}""".stripMargin
    )

    val coll =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll.active.get.collIndexes.map(_.indexID) shouldBe Nil
    coll.staged.get.collIndexes.map(_.indexID) shouldBe List(IndexID(32768))

    // This update keeps the `byName` index around, even though the fields and
    // migrations are different.
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name_2: String
           |  email: String
           |
           |  migrations {
           |    move .name -> .name_2
           |  }
           |
           |  index byName {
           |    terms [.name_2]
           |  }
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}""".stripMargin
    )

    val coll1 =
      ctx ! Collection.getUncached(auth.scopeID, CollectionID(1024)).map(_.get)

    coll1.active.get.collIndexes.map(_.indexID) shouldBe Nil
    coll1.staged.get.collIndexes.map(_.indexID) shouldBe List(
      IndexID(32768),
      IndexID(32769))
  }

  "can create a new collection in a staged push" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    pin()
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |collection Post {
           |  title: String
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'A' })")
    // `Post` is staged, so it cannot be accessed.
    renderErr(auth, "Post.create({ title: 'A' })") shouldBe (
      """|error: Unbound variable `Post`
         |at *query*:1:1
         |  |
         |1 | Post.create({ title: 'A' })
         |  | ^^^^
         |  |""".stripMargin
    )
    // Name lookups are a bit different when typechecking is off, so make sure this
    // behaves the same.
    renderErr(auth, "Post.create({ title: 'A' })", typecheck = false) shouldBe (
      """|error: Unbound variable `Post`
         |at *query*:1:1
         |  |
         |1 | Post.create({ title: 'A' })
         |  | ^^^^
         |  |""".stripMargin
    )

    commit()

    evalOk(auth, "User.create({ name: 'A' })")
    evalOk(auth, "Post.create({ title: 'A' })")
  }

  "can create a new collection in FQL during a staged push" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    pin()

    evalOk(auth, "Collection.create({ name: 'Post' })")

    evalOk(auth, "User.create({ name: 'A' })")
    // `Post` is staged, so it cannot be accessed.
    renderErr(auth, "Post.create({ title: 'A' })") shouldBe (
      """|error: Unbound variable `Post`
         |at *query*:1:1
         |  |
         |1 | Post.create({ title: 'A' })
         |  | ^^^^
         |  |""".stripMargin
    )

    commit()

    evalOk(auth, "User.create({ name: 'A' })")
    evalOk(auth, "Post.create({ title: 'A' })")
  }

  "can create a function in a staged push" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    pin()
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |function add(a: Number, b: Number): Number {
           |  a + b
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'A' })")
    // `add` is staged, so it cannot be accessed.
    renderErr(auth, "add(1, 2)") shouldBe (
      """|error: Unbound variable `add`
         |at *query*:1:1
         |  |
         |1 | add(1, 2)
         |  | ^^^
         |  |""".stripMargin
    )

    commit()

    evalOk(auth, "User.create({ name: 'A' })")
    evalOk(auth, "if (add(1, 2) != 3) abort(0)")
  }

  "can rename collections" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Foo {}
           |collection Bar {}""".stripMargin)

    pin()

    evalOk(auth, "Collection.byName('Bar')!.update({ name: 'Baz' })")

    // Old name should still be used for live queries.
    evalOk(auth, "Foo.all()")
    evalOk(auth, "Bar.all()")

    // New name should show up when using `Collection.all()`.
    evalOk(
      auth,
      "if (Collection.all().map(.name).toArray() != ['Foo', 'Baz']) abort(0)")

    // New name should be used for `Collection.byName`.
    evalOk(auth, "Collection.byName('Foo')!")
    evalOk(auth, "Collection.byName('Baz')!")
    evalOk(auth, "if (Collection.byName('Bar').exists()) abort(0)")
  }

  "don't delete backing indexes when staging updates" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'Alice' })")

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    // The backing index shouldn't actually be deleted.
    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1

    evalOk(auth, "if (User.byName('Alice').count() != 1) abort(0)")
  }

  "can stage creating a collection" in {
    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    commit()

    evalOk(auth, "User.create({ name: 'Alice' })")
    evalOk(auth, "if (User.byName('Alice').count() != 1) abort(0)")
  }

  "can abandon an empty collection" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin)

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |  email: String
           |}""".stripMargin)

    abandon()

    evalOk(
      auth,
      "if (User.definition.fields != { name: { signature: 'String' } }) abort(0)")
  }

  "can abandon a staged collection create" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin)

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}
           |
           |collection Foo {
           |  bar: String
           |}
           |""".stripMargin
    )

    evalOk(auth, "if (Collection.all().count() != 2) abort(0)")

    abandon()

    evalOk(auth, "if (Collection.all().count() != 1) abort(0)")
  }

  "don't delete backing indexes when they can be reused for an empty collection" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1
  }

  "don't delete backing indexes for unique constraints when they can be reused for an empty collection" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1
  }

  "reuse active backing indexes for a staged unique constraint" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  unique [.name]
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ name: 'Alice' })")

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name2: String
           |
           |  migrations {
           |    move .name -> .name2
           |  }
           |
           |  unique [.name2]
           |}""".stripMargin
    )

    // The same index should get re-used.
    (ctx ! SchemaCollection.Index(auth.scopeID).allIDs().flattenT) shouldBe Seq(
      IndexID(32768))

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name2: String
           |
           |  migrations {
           |    move .name -> .name2
           |  }
           |
           |  unique [.name2]
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allIDs().flattenT) shouldBe Seq(
      IndexID(32768))
  }

  "don't delete backing indexes when they can be reused for a collection with a doc" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin
    )

    evalOk(auth, "User.create({ id: 3, name: 'Alice' })")

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}""".stripMargin
    )

    (ctx ! SchemaCollection.Index(auth.scopeID).allDocs().flattenT).length shouldBe 1

    commit()
    evalOk(auth, "if (User.byName('Alice').first()!.id != 3) abort(0)")
  }

  case class IndexDef(terms: Seq[String], values: Seq[String] = Seq.empty)

  def indexesForColl(coll: String) = {
    ctx ! (for {
      collID <- Collection.idByNameActive(auth.scopeID, coll).map(_.get)
      coll   <- Collection.get(auth.scopeID, collID).map(_.get)
      vers   <- SchemaCollection.Collection(auth.scopeID).get(collID).map(_.get)
    } yield {
      // These are all the indexes that the collection has references to.
      val mgr = CollectionIndexManager(auth.scopeID, collID, vers.data)

      val defs = mgr.backingIndexes.map { idx =>
        IndexDef(idx.terms.map(_.field.path), idx.values.map(_.field.path))
      }

      // These are all the index documents pointing to this collection.
      val ids = coll.config.indexConfigs
        .filter(_.isCollectionIndex)
        .filter(_.sources.contains(collID))
        .map(_.id)

      // The above two should match.
      if (mgr.backingIndexes.map(_.indexID) != ids) {
        println(s"expected the following defs: ${defs.mkString("\n")}")
        val indexes = ids.map { id =>
          val doc = (ctx ! SchemaCollection.Index(auth.scopeID).get(id)).get
          (
            doc.id,
            doc.data.fields.get(List("terms")),
            doc.data.fields.get(List("values")))
        }
        println(s"but found the following indexes: ${indexes.mkString("\n")}")
        fail()
      }

      defs
    })
  }

  "pushing the same schema twice should cleanup indexes" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection Account {
           |  index by_account_id {
           |    terms [.account_id]
           |    values [.account_id, .jurisdiction_id]
           |  }
           |  index by_jurisdiction_id {
           |    terms [.jurisdiction_id]
           |    values [.jurisdiction_id, .account_id]
           |  }
           |  unique [.account_id]
           |  unique [.jurisdiction_id]
           |}""".stripMargin
    )

    indexesForColl("Account") shouldBe List(
      IndexDef(
        terms = Seq(".jurisdiction_id"),
        values = Seq(".jurisdiction_id", ".account_id")),
      IndexDef(
        terms = Seq(".account_id"),
        values = Seq(".account_id", ".jurisdiction_id")),
      IndexDef(terms = Seq(".account_id")),
      IndexDef(terms = Seq(".jurisdiction_id"))
    )

    evalOk(
      auth,
      """|Set.sequence(0, 256).forEach((_) => {
         |  Account.create({})
         |})""".stripMargin
    )

    val schema2 = (
      """|collection Account {
         |  jurisdiction_id: String
         |  account_id: String
         |  branding_updated_timestamp: Time?
         |
         |  // catch all for any ad-hoc fields and existing field type conflicts.
         |  type_conflicts: { *: Any }?
         |
         |  // legacy wildcard that will be removed in a future migration, but can't be removed in
         |  // the first migration.
         |  *: Any
         |
         |  index by_account_id {
         |    terms [.account_id]
         |    values [.account_id, .jurisdiction_id]
         |  }
         |
         |  index by_jurisdiction_id {
         |    terms [.jurisdiction_id]
         |    values [.jurisdiction_id, .account_id]
         |  }
         |
         |  unique [.account_id]
         |  unique [.jurisdiction_id]
         |
         |  migrations {
         |    // start enforcing schema.
         |    add .jurisdiction_id
         |    backfill .jurisdiction_id = "ERROR"
         |    add .account_id
         |    backfill .account_id = "ERROR"
         |    add .branding_updated_timestamp
         |    add .type_conflicts
         |    move_conflicts .type_conflicts
         |  }
         |}""".stripMargin
    )

    updateSchemaPinOk(auth, "main.fsl" -> schema2)
    // All the indexes need to be rebuilt, so add new ones for the staged schema.
    (ctx ! SchemaCollection.Index(auth.scopeID).allIDs().flattenT).length shouldBe 8

    updateSchemaPinOk(auth, "main.fsl" -> schema2)
    // The 4 new indexes will be rebuilt a second time (even though the migrations
    // are the same, we have no optimized this path to reuse the previously staged
    // indexes yet).
    (ctx ! SchemaCollection.Index(auth.scopeID).allIDs().flattenT).length shouldBe 8

    abandon()

    indexesForColl("Account") shouldBe List(
      IndexDef(
        terms = Seq(".jurisdiction_id"),
        values = Seq(".jurisdiction_id", ".account_id")),
      IndexDef(
        terms = Seq(".account_id"),
        values = Seq(".account_id", ".jurisdiction_id")),
      IndexDef(terms = Seq(".account_id")),
      IndexDef(terms = Seq(".jurisdiction_id"))
    )
  }

  "staged access providers don't blow up" in {
    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|access provider foo {
           |  issuer "https://fauna.auth0.com"
           |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
           |  role my_role
           |}
           |
           |role my_role {}""".stripMargin
    )

    val auth2 =
      ctx ! AccessProvider.getByIssuerUncached(
        auth.scopeID,
        "https://fauna.auth0.com")

    // The access provider is staged, so we shouldn't find anything.
    auth2 shouldBe None
  }

  "making a collection non-empty is not allowed with a staged schema" in {
    // Blank schema, allows any fields.
    updateSchemaOk(auth, "main.fsl" -> "collection User {}")

    // Allowed, because the collection is empty.
    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin)

    // Makes the collection non-empty! And, because the active schema to enforce this
    // query, we currently allow this.
    evalOk(auth, "User.create({ id: 0, foo: 3 })")

    // The staged schema is no longer valid.
    val errs = commitErr()
    // TODO: Make this easier to test
    errs.length shouldBe 1
    val errors = errs.head.asInstanceOf[SchemaError.SchemaSourceErrors]
    val fqlError = errors.errors.head.asInstanceOf[SchemaError.FQLError]
    val rendered = fqlError.renderWithSource(errors.files.map { f =>
      f.src -> f.content
    }.toMap)

    rendered shouldBe (
      """|error: Field `.name` is not present in the live schema
         |at main.fsl:2:3
         |  |
         |2 |   name: String
         |  |   ^^^^
         |  |
         |hint: Provide an `add` migration for this field
         |  |
         |2 |     migrations {
         |  |  ___+
         |3 | |     add .name
         |4 | |     backfill .name = <expr>
         |5 | |   }
         |  | |____^
         |  |
         |hint: Add a default value to this field
         |  |
         |2 |   name: String = <expr>
         |  |               +++++++++
         |  |
         |hint: Make the field nullable
         |  |
         |2 |   name: String?
         |  |               +
         |  |""".stripMargin
    )
  }

  "migration task should not be kicked off from staged push" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |}""".stripMargin)

    evalOk(auth, "User.create({ id: 0, name: 'Alice' })")

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|collection User {
           |  name: String
           |
           |  migrations {
           |    drop .name
           |    add .name
           |    backfill .name = "foo"
           |  }
           |}""".stripMargin
    )

    evalOk(auth, "User(0)!.name").as[String] shouldBe "Alice"

    commit()

    evalOk(auth, "User(0)!.name").as[String] shouldBe "foo"
  }
}
