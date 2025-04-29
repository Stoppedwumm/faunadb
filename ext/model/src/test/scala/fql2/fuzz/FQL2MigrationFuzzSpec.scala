package fauna.model.test

import fauna.repo.values.Value
import fauna.repo.values.ValueReification
import fql.ast.display._
import org.scalacheck.Prop

object FQL2MigrationFuzzSpec extends FQL2SchemaFuzzSpec("migration") {
  property("migration") = Prop.forAllNoShrink(applySeriesOfMigrations()) {
    case (base, migrated) =>
      val auth = newDB

      // Migrate.
      updateOk(auth, "main.fsl" -> base.schema.display)
      val name = base.schema.name.str
      evalOk(auth, s"$name.create(${base.row.display})")
      validateModelCollection(auth, base.schema)
      updateOk(auth, "main.fsl" -> migrated.schema.display)
      validateModelCollection(auth, migrated.schema)

      // Check the row migrated by the test and the row migrated by the system agree.
      val (_, fuzz) =
        ValueReification.reify(evalOk(auth, s"${migrated.row.display}"))
      val (_, auto) =
        ValueReification.reify(evalOk(auth, s"$name.all().first()!.data", typecheck = false))

      if (fuzz != auto) {
        println(s"${base.schema.display}")
        println(s"${migrated.schema.display}")
        println(s"${base.row.display}")
        println(s"${migrated.row.display}")
        println(fuzz)
        println(auto)
        throw new IllegalStateException("rows didn't match")
      }

      // Check indexed fields with non-null values have an index entries.
      migrated.schema.indexes.foreach { idx =>
        val field = idx.name.str.drop(2) // Subtract "by".
        val e = migrated.fields(field).expr
        if (!isNullExpr(e)) {
          val v = evalOk(auth, s"$name.by$field(${e.display}).count()")
          if (v != Value.Int(1)) {
            throw new IllegalStateException("missing index entry")
          }
        }
      }

      PropTestResult.ValidTestPassed
  }
}
