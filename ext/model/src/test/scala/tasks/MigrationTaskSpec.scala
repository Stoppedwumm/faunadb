package fauna.model.test

import fauna.atoms.{ AccountID, DocID, ScopeID }
import fauna.exec.FaunaExecutionContext
import fauna.lang.clocks.Clock
import fauna.model.{ Collection, RuntimeEnv }
import fauna.model.tasks.{ MigrationTask, TaskExecutor, TaskRouter }
import fauna.model.Task
import fauna.repo.{ PagedQuery, Store }
import fauna.repo.doc.Version
import fauna.repo.schema.CollectionSchema
import fauna.repo.service.rateLimits.FixedWriteOpsLimiter
import fauna.storage.{ ScanBounds, Selector, VersionID }
import fauna.storage.ir._
import io.netty.buffer.Unpooled
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class MigrationTaskSpec extends FQL2Spec {

  // Assumes the collection exists and is named 'Foo'.
  private def checkQ(scope: ScopeID) = for {
    schema <- schemaQ(scope)
    docs   <- allVersionsUnmigratedQ(schema).flattenT
    mdocs  <- allVersionsQ(schema).flattenT
  } yield {
    docs shouldEqual mdocs
  }

  // Ditto assumptions.
  def schemaQ(scope: ScopeID) = for {
    id <- Collection.idByNameActive(scope, "Foo")
    // Cache bad -.-.
    schema <- RuntimeEnv.InlineIndexEnv.getCollection(scope, id.get).map {
      _.get.Schema
    }
  } yield schema

  private def allVersionsQ0(
    schema: CollectionSchema,
    versions: DocID => PagedQuery[Iterable[Version]]
  ) =
    Store
      .docScanRaw(
        ScanBounds.All,
        Unpooled.EMPTY_BUFFER,
        Selector.from(schema.scope, Seq(schema.collID)),
        pageSize = 2)
      .flatMapPagesT { doc =>
        versions(doc.docID) mapValuesT { v =>
          ((v.parentScopeID, v.id), v)
        }
      }

  private def allVersionsQ(schema: CollectionSchema) = allVersionsQ0(
    schema,
    Store.versions(
      schema,
      _,
      VersionID.MaxValue,
      VersionID.MinValue,
      pageSize = 10,
      reverse = false))

  private def allVersionsUnmigratedQ(schema: CollectionSchema) = {
    val empty = CollectionSchema.empty(schema.scope, schema.collID)
    allVersionsQ0(
      empty,
      Store.versions(
        empty,
        _,
        VersionID.MaxValue,
        VersionID.MinValue,
        pageSize = 10,
        reverse = false))
  }

  "Synchronous migration" - {
    "works" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |}""".stripMargin
      )

      val q0 = (1 to MigrationTask.Sync.MaxVersions / 2)
        .map { i =>
          s"Foo.create({ a: $i })"
        }
        .mkString("\n")
      evalOk(auth, q0)

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |  b: Int
             |
             |  migrations {
             |    add .b
             |    backfill .b = 11
             |  }
             |}""".stripMargin
      )

      // Insert more docs (up to the synchronous limit).
      val q1 = (1 to MigrationTask.Sync.MaxVersions / 2)
        .map { i =>
          s"Foo.create({ a: $i, b: ${2 * i + 1} })"
        }
        .mkString("\n")
      evalOk(auth, q1)

      // Synchronous: no tasks.
      import TaskHelpers._
      (ctx ! tasks).isEmpty shouldBe true

      // Migration happened.
      ctx ! checkQ(auth.scopeID)
    }

    "ignores rate limits for sync builds" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |}""".stripMargin
      )

      val q0 = (1 to MigrationTask.Sync.MaxVersions)
        .map { i =>
          s"Foo.create({ a: $i })"
        }
        .mkString("\n")
      evalOk(auth, q0)

      // This is a shameless rip-off of updateSchema0.
      // While gross, this seems cleaner than plumbing some limiter
      // parameter that no other tests will use.
      val files = "main.fsl" ->
        """|collection Foo {
           |  a: Int
           |  b: Int
           |
           |  migrations {
           |    add .b
           |    backfill .b = 11
           |  }
           |}""".stripMargin

      // This is definitely too few write ops if sync migration writes count.
      val limiter = FixedWriteOpsLimiter(MigrationTask.Sync.MaxVersions / 2)
      noException shouldBe thrownBy(
        Await.result(
          ctx.run(
            updateSchema0Q(auth, overrideMode = true, pin = false, files),
            Clock.time,
            AccountID.Root,
            auth.scopeID,
            limiter),
          1.minute))

      // Synchronous: no tasks.
      import TaskHelpers._
      (ctx ! tasks).isEmpty shouldBe true

      // Migration happened.
      ctx ! checkQ(auth.scopeID)
    }
  }

  "The migration task" - {
    s"works by index" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
                 |  a: Int
                 |  b: Int | String
                 |  d: String
                 |}""".stripMargin
      )

      // Insert a bunch of docs.
      val n = MigrationTask.Sync.MaxVersions + 1
      (0 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            val b = if (n % 2 == 0) n / 2 else s"$n"
            s"Foo.create({ a: $n, b: $b, d: 'goodbye'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
                 |  a: Int
                 |  b: Int
                 |  c: String
                 |
                 |  migrations {
                 |    drop .d
                 |    split .b -> .b, .c
                 |    backfill .b = 0
                 |    backfill .c = "-"
                 |  }
                 |}""".stripMargin
      )

      // Insert more docs.
      (5 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            s"Foo.create({ a: $n, b: $n, c: '$n'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      import TaskHelpers._
      val exec = TaskExecutor(ctx)

      // Check the root.
      (ctx ! tasks).size shouldBe 1
      val root = ((ctx ! tasks) find { _.name == MigrationTask.Root.name }).get
      root.data(MigrationTask.StateField) shouldBe "fork"

      (ctx ! tasks).size shouldBe 1
      inside(task(ctx, root.id).state) { case Task.Runnable(data, _) =>
        data(MigrationTask.StateField) shouldBe "fork"
      }

      // Fork then check the child(ren).
      exec.step()
      (ctx ! tasks).size shouldBe 1
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        ids.size shouldBe 1
        data(MigrationTask.StateField) shouldBe "join"
      }

      // Migrate.
      processAllTasks(ctx, exec)
      (ctx ! tasks).isEmpty shouldBe true

      // Check the migration migrated.
      ctx ! checkQ(auth.scopeID)
    }

    s"works by scan" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
                 |  a: Int
                 |  b: Int | String
                 |  d: String
                 |}""".stripMargin
      )

      // Insert a bunch of docs.
      val n = MigrationTask.Sync.MaxVersions + 1
      (0 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            val b = if (n % 2 == 0) n / 2 else s"$n"
            s"Foo.create({ a: $n, b: $b, d: 'goodbye'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
                 |  a: Int
                 |  b: Int
                 |  c: String
                 |
                 |  migrations {
                 |    drop .d
                 |    split .b -> .b, .c
                 |    backfill .b = 0
                 |    backfill .c = "-"
                 |  }
                 |}""".stripMargin
      )

      // Insert more docs.
      (5 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            s"Foo.create({ a: $n, b: $n, c: '$n'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      import TaskHelpers._
      val exec = TaskExecutor(ctx)

      // Check the root.
      (ctx ! tasks).size shouldBe 1
      val root = ((ctx ! tasks) find { _.name == MigrationTask.Root.name }).get
      root.data(MigrationTask.StateField) shouldBe "fork"

      // Fork, then cancel the child, which will be a by-index root.
      exec.step()
      (ctx ! tasks).size shouldBe 1
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        data(MigrationTask.StateField) shouldBe "join"
        ids.size shouldBe 1
        val byIndexRoot = task(ctx, ids.head)
        byIndexRoot.name shouldBe MigrationTask.ByIndex.Root.name
        ctx ! byIndexRoot.cancel(None)
      }

      // Cancelling the by-index root then stepping should re-fork the root
      // into scan tasks.
      exec.step()
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        data(MigrationTask.StateField) shouldBe "join"
        ids.foreach { task(ctx, _).name shouldBe MigrationTask.Scan.name }
      }

      // Migrate.
      processAllTasks(ctx, exec)
      (ctx ! tasks).isEmpty shouldBe true

      // Check the migration migrated.
      ctx ! checkQ(auth.scopeID)
    }

    s"scans require constant topology version" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |  b: Int | String
             |  d: String
             |}""".stripMargin
      )

      // Insert a bunch of docs.
      val n = MigrationTask.Sync.MaxVersions + 1
      (0 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            val b = if (n % 2 == 0) n / 2 else s"$n"
            s"Foo.create({ a: $n, b: $b, d: 'goodbye'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |  b: Int
             |  c: String
             |
             |  migrations {
             |    drop .d
             |    split .b -> .b, .c
             |    backfill .b = 0
             |    backfill .c = "-"
             |  }
             |}""".stripMargin
      )

      // Insert more docs.
      (5 to n).grouped(25).foreach { ns =>
        val q = ns
          .map { n =>
            s"Foo.create({ a: $n, b: $n, c: '$n'})"
          }
          .mkString("\n")
        evalOk(auth, q)
      }

      import TaskHelpers._
      val exec = TaskExecutor(ctx)

      // Check the root.
      (ctx ! tasks).size shouldBe 1
      val root = ((ctx ! tasks) find { _.name == MigrationTask.Root.name }).get
      root.data(MigrationTask.StateField) shouldBe "fork"

      // Fork, then cancel the child, which will be a by-index root.
      exec.step()
      (ctx ! tasks).size shouldBe 1
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        data(MigrationTask.StateField) shouldBe "join"
        ids.size shouldBe 1
        val byIndexRoot = task(ctx, ids.head)
        byIndexRoot.name shouldBe MigrationTask.ByIndex.Root.name
        ctx ! byIndexRoot.cancel(None)
      }

      // Cancelling the by-index root then stepping should re-fork the root
      // into scan tasks.
      exec.step()
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, data, _) =>
        data(MigrationTask.StateField) shouldBe "join"
        ids.foreach { task(ctx, _).name shouldBe MigrationTask.Scan.name }
      }

      // Doing a no-op rebalance will increase the topology version.
      val oldVersion = ctx.service.partitioner.partitioner.version
      ctx.service.balanceReplica(ctx.service.replicaName)
      eventually(timeout(10.seconds)) {
        val newVersion = ctx.service.partitioner.partitioner.version
        newVersion > oldVersion shouldBe true
      }

      // Stepping the scan pauses it because of the topology version change.
      exec.step()
      inside(task(ctx, root.id).state) { case Task.Blocked(ids, _, _) =>
        ids.foreach { id =>
          val t = task(ctx, id)
          t.state should matchPattern {
            case Task.Paused(
                  "topology version change detected: parent must be repartitioned or restarted",
                  _,
                  _
                ) => // OK.
          }
        }
      }
    }

    "obeys the lifecycle rules" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |}""".stripMargin
      )

      // Add enough docs to dodge the synchronous path.
      val q = (1 to (MigrationTask.Sync.MaxVersions + 1))
        .map { i =>
          s"Foo.create({ a: $i })"
        }
        .mkString("\n")
      evalOk(auth, q)

      // Add migrations.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  b: Int
             |
             |  migrations {
             |    move .a -> .b
             |  }
             |}""".stripMargin
      )

      evalOk(auth, "Foo.create({ b: 1 })")

      import TaskHelpers._
      val exec = TaskExecutor(ctx)
      (ctx ! tasks).size shouldBe 1
      processAllTasks(ctx, exec)
      (ctx ! tasks).isEmpty shouldBe true

      // Check the task migrated things.
      ctx ! checkQ(auth.scopeID)

      // Change the collection in a way that does not require migration.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  b: Int
             |
             |  compute bb = (doc) => 2 * doc.b + 1
             |
             |  migrations {
             |    move .a -> .b
             |  }
             |}""".stripMargin
      )

      // No migration task should have been spawned.
      (ctx ! tasks).isEmpty shouldBe true

      // Add migrations to spawn another task.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  b: Int
             |  c: Int
             |
             |  migrations {
             |    move .a -> .b
             |    add .c
             |    backfill .c = 11
             |  }
             |}""".stripMargin
      )

      (ctx ! tasks).size shouldBe 1
      val orig = (ctx ! tasks).head.id

      // Add more migrations. The original task should be replaced.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  b: Int
             |  c: Int
             |  d: String = ""
             |
             |  migrations {
             |    move .a -> .b
             |    add .c
             |    backfill .c = 11
             |    add .d
             |  }
             |}""".stripMargin
      )
      (ctx ! tasks).size shouldBe 1
      (ctx ! tasks).head.id should not equal (orig)

      // Delete the collection. The task should be cancelled.
      updateSchemaOk(
        auth,
        "main.fsl" -> ""
      )

      (ctx ! tasks).isEmpty shouldBe true
    }

    "has lower priority than index builds" in {
      val auth = newDB

      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  a: Int
             |  *: Any
             |}""".stripMargin
      )

      // Add enough docs to dodge the synchronous path.
      val q = (1 to (MigrationTask.Sync.MaxVersions + 1))
        .map { i =>
          s"Foo.create({ a: $i })"
        }
        .mkString("\n")
      evalOk(auth, q)

      // We'd need 100 index builds to hit the limit of deprioritization... let's
      // just do 5 and check things basically work.
      updateSchemaOk(
        auth,
        "main.fsl" ->
          """|collection Foo {
             |  *: Any
             |
             |  index byX0 { terms [.x0] }
             |  index byX1 { terms [.x1] }
             |  index byX2 { terms [.x2] }
             |  index byX3 { terms [.x3] }
             |  index byX4 { terms [.x4] }
             |
             |  migrations {
             |    drop .a
             |  }
             |}""".stripMargin
      )

      import TaskHelpers._
      (ctx ! tasks).size shouldBe (5 + 1)
      val mprio =
        ctx ! TaskRouter.customPriorityForTask("migration-root", AccountID(0))
      val iprio =
        ctx ! TaskRouter.customPriorityForTask("index-build-join", AccountID(0))
      (mprio < iprio) shouldBe true
    }

    "works with concurrent writes" in {
      val auth = newDB

      // Assumes there's at least one step, so a1 exists.
      def checkDoc(version: Version) = {
        def long(ir: IRValue) = ir match {
          case LongV(i) => i
          case _        => fail("not a long?!")
        }

        val fs = version.data.fields.elems.toMap
        val a0 = fs.get("a0").fold(-1L)(long)
        val v = fs.get("a1").fold(-1L)(long)
        a0 * (a0 - v) shouldBe 0
        var i = 1
        while (fs.contains(s"a$i")) {
          fs.get(s"a$i").fold(-1L)(long) shouldBe v
          i += 1
        }
        i shouldBe v
      }

      // Checks docs are consistent with themselves.
      // Should always pass, regardless of the progress of writes, migrations,
      // and background tasks.
      def checkDocs() = {
        val q = for {
          schema <- schemaQ(auth.scopeID)
          docs   <- allVersionsQ(schema).flattenT
        } yield docs
        val docs = ctx ! q
        docs.foreach { d => checkDoc(d._2) }
      }

      // Write `w` documents at version `v`.
      def doWrites(v: Int, w: Int) = {
        val doc = (0 to v)
          .map { i =>
            s"a$i: $v"
          }
          .mkString("{", ", ", "}")
        (1 to w).foreach { _ =>
          // Do the writes one at a time so it takes a bit.
          evalOk(
            auth,
            s"Foo.create($doc)"
          )
        }
      }

      // Apply version number `v`. The schema is tuned so that if migrations are
      // applied out of order the documents will not be as expected.
      def doSchemaUpdate(v: Int) = {
        val fields = (0 to v)
          .map { i =>
            s"  a$i: Number"
          }
          .mkString("\n")

        val migrations = {

          val ms = (1 to v)
            .map { i =>
              s"""|    move .a0 -> .a$i
                  |    add .a0
                  |    backfill .a0 = 0""".stripMargin
            }
            .mkString("\n")

          if (v == 0) {
            ""
          } else {
            s"""|  migrations {
                |$ms
                |  }""".stripMargin
          }
        }

        updateSchemaOk(
          auth,
          "main.fsl" ->
            s"""|collection Foo {
                |$fields
                |
                |$migrations
                |}""".stripMargin
        )
      }

      import FaunaExecutionContext.Implicits.global

      // Repeatedly check document self-consistency concurrent with other activity.
      val check = new AtomicBoolean(true)
      val checkF = Future {
        while (check.get()) {
          // Stupid scheduling. Raise if doc number becomes large.
          Thread.sleep(50)
          checkDocs()
        }
      }

      // Tuned low for unit tests, but can be raised for more rigor.
      val mv = 5
      val w = 150 // > 128 so migrations are in the background from the beginning.
      (0 to mv).foreach { v =>
        doSchemaUpdate(v)

        // Run the migration task concurrently with the writes.
        // TODO: Could skip it for some versions (skip checkQ too).
        val taskF = Future {
          import TaskHelpers._
          val exec = TaskExecutor(ctx)
          processAllTasks(ctx, exec)
          (ctx ! tasks).isEmpty shouldBe true
          ()
        }

        doWrites(v, w)
        Await.ready(taskF, 10.minutes)
        ctx ! checkQ(auth.scopeID)
      }
      check.set(false)
      Await.ready(checkF, 10.minutes)
    }
  }
}
