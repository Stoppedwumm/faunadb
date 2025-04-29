package fauna.model.test

import fauna.model.schema.index.CollectionIndex
import fauna.model.tasks.TaskExecutor
import fauna.model.Index
import fauna.repo.VersionIndexEntriesTooLargeException

class FQL2IndexBombSpec extends FQL2Spec {
  val bomba: String = {
    val terms = (1 to 100 map { i => s"'$i tttttttt'" }).mkString("[", ",", "]")
    val values = (1 to 100 map { i => s"'$i vvvvvvvv'" }).mkString("[", ",", "]")
    s"{ f1: $terms, f2: $values, f3: $values }"
  }

  val index: String =
    """|byF1: {
       |  terms: [{ field: ".f1", mva: true }],
       |  values: [{ field: ".f2", mva: true }, { field: ".f3", mva: true }]
       |}""".stripMargin

  "an index bomb" - {
    "is defused when inserted into an existing index" in {
      val auth = newDB

      evalOk(
        auth,
        s"""|Collection.create({
            |  name: "Foo",
            |  indexes: { $index }
            |})""".stripMargin
      )

      // Drop the bomb.
      assertThrows[VersionIndexEntriesTooLargeException] {
        evalErr(auth, s"Foo.create($bomba)")
      }
    }

    "is defused when an index is sync built over it" in {
      val auth = newDB

      evalOk(auth, "Collection.create({ name: 'Foo' })")
      evalOk(auth, s"Foo.create($bomba)")

      // Step on the bomb.
      assertThrows[VersionIndexEntriesTooLargeException] {
        evalErr(
          auth,
          s"""|Foo.definition.update({
              |  indexes: { $index }
              |})""".stripMargin
        )
      }
    }

    "is defused when an index is async built over it" in {
      val auth = newDB

      evalOk(auth, "Collection.create({ name: 'Foo' })")
      evalOk(auth, s"Foo.create($bomba)")
      evalOk(
        auth,
        (1 to Index.BuildSyncSize).map { _ => "Foo.create({})" }.mkString(";")
      )

      evalOk(
        auth,
        s"""|Foo.definition.update({
            |  indexes: { $index }
            |})""".stripMargin
      )

      // Make some other guy step on the bomb while you're not looking.
      // (Run the build and check no index was produced.)
      val executor = TaskExecutor(ctx)
      while (ctx ! executor.runQueue(ctx.service.localID.get).nonEmptyT)
        executor.step()
      evalErr(
        auth,
        "Foo.byF1('nope')").failureMessage shouldBe "The index `Foo.byF1` is not queryable."
      pendingUntilFixed {
        // The task should fail, but it gets stuck in "building" status...
        evalOk(auth, "Foo.definition.indexes.byF1?.status")
          .as[String] shouldBe CollectionIndex.Status.Failed.asStr
      }
    }
  }
}
