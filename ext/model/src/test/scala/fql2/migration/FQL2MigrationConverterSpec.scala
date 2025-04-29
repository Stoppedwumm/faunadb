package fauna.model.test

import fauna.auth._
import fauna.lang.syntax._
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.runtime.fql2.Result
import fauna.model.schema.migration.MigrationConverter
import fauna.repo.schema.migration.Migration
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._
import fauna.storage.doc.ConcretePath
import fauna.storage.ir._
import fql.{ Result => FQLResult }
import fql.ast._
import fql.migration._
import fql.parser._
import scala.concurrent.duration._

class FQL2MigrationConverterSpec extends FQL2Spec {
  var auth: Auth = _

  before {
    auth = newDB
  }

  implicit val deadline = scala.Int.MaxValue.seconds.fromNow

  def derive(from: String, to: String)(
    implicit pos: org.scalactic.source.Position) = {
    val fromItem =
      Parser
        .schemaItems(from, Src.Inline("main.fsl", from))
        .getOrElse(fail())
        .head
        .asInstanceOf[SchemaItem.Collection]
    val toItem = Parser
      .schemaItems(to, Src.Inline("main.fsl", to))
      .getOrElse(fail())
      .head
      .asInstanceOf[SchemaItem.Collection]

    val typer = Global.StaticEnv.newTyper()

    MigrationValidator.validate(typer, fromItem, toItem) match {
      case FQLResult.Ok(migrations) => migrations
      case FQLResult.Err(errs) =>
        fail(errs.map(_.renderWithSource(Map.empty)).mkString("\n"))
    }
  }

  def convert(from: String, to: String, expected: Seq[Migration])(
    implicit pos: org.scalactic.source.Position) = {
    val migrations = derive(from, to)
    val converter = ctx ! MigrationConverter(auth.scopeID)
    val internal = ctx ! migrations.map(converter.fromFSL).sequenceT
    internal match {
      case Result.Ok(migrations) =>
        val actual = migrations.flatten
        if (actual != expected) {
          fail(
            s"Expected:\n  ${expected.mkString("\n  ")}\nActual:\n  ${actual.mkString("\n  ")}")
        }
      case Result.Err(err) =>
        fail(err.errors.map(_.renderWithSource(Map.empty)).mkString("\n"))
    }
  }

  def convertErr(from: String, to: String, expected: String)(
    implicit pos: org.scalactic.source.Position) = {
    val migrations = derive(from, to)
    val converter = ctx ! MigrationConverter(auth.scopeID)
    val internal = ctx ! migrations.map(converter.fromFSL).sequenceT
    internal match {
      case Result.Ok(migrations) =>
        fail(
          s"Expected failure, got migrations:\n  ${migrations.flatten.mkString("\n  ")}")
      case Result.Err(err) =>
        val actual = err.errors.map(_.renderWithSource(Map.empty)).mkString("\n")
        if (actual != expected) {
          fail(s"Expected:\n$expected\nActual:\n$actual")
        }
    }
  }

  "it converts add and drop migrations" in {
    convert(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  bar: Int = 3
         |
         |  migrations {
         |    drop .foo
         |    add .bar
         |    backfill .bar = 3
         |  }
         |}""".stripMargin,
      Seq(
        Migration.DropField(ConcretePath("data", "foo")),
        Migration.AddField(ConcretePath("data", "bar"), Int, 3)
      )
    )
  }

  "it converts narrow migrations" in {
    convert(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 0
         |
         |  migrations {
         |    split .foo -> .foo, .tmp
         |    drop .tmp
         |  }
         |}""".stripMargin,
      Seq(
        Migration.SplitField(
          ConcretePath("data", "foo"),
          ConcretePath("data", "tmp"),
          Int,
          0,
          NullV),
        Migration.DropField(ConcretePath("data", "tmp"))
      )
    )

    convert(
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | Null
         |
         |  migrations {
         |    split .foo -> .foo, .tmp
         |    drop .tmp
         |  }
         |}""".stripMargin,
      Seq(
        Migration.SplitField(
          ConcretePath("data", "foo"),
          ConcretePath("data", "tmp"),
          Union(Int, Null),
          NullV,
          NullV),
        Migration.DropField(ConcretePath("data", "tmp"))
      )
    )
  }

  "it converts widen migrations" in {
    convert(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | String
         |}""".stripMargin,
      Seq.empty
    )

    convert(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int | String = 0
         |}""".stripMargin,
      Seq.empty
    )

    convert(
      """|collection User {
         |  foo: Int
         |}""".stripMargin,
      """|collection User {
         |  foo: Int = 0
         |}""".stripMargin,
      Seq.empty
    )
  }

  "it errors for non-persistable types" in {
    convertErr(
      """|collection User {
         |  foo: Int | ID | Null
         |}""".stripMargin,
      """|collection User {
         |  foo: ID | Null
         |
         |  migrations {
         |    split .foo -> .foo, .tmp
         |    drop .tmp
         |  }
         |}""".stripMargin,
      """|error: Failed to convert migration.
         |constraint failures:
         |  foo: Type cannot be persisted.
         |at main.fsl:2:8
         |  |
         |2 |   foo: ID | Null
         |  |        ^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "it errors for non-persistable values" in {
    convertErr(
      """|collection User {
         |  _no_wildcard: Null
         |}""".stripMargin,
      """|collection User {
         |  foo: Any = () => true
         |  _no_wildcard: Null
         |
         |  migrations {
         |    add .foo
         |  }
         |}""".stripMargin,
      """|error: Failed to convert migration.
         |constraint failures:
         |  Value has type Lambda, which cannot be persisted.
         |at main.fsl:2:14
         |  |
         |2 |   foo: Any = () => true
         |  |              ^^^^^^^^^^
         |  |""".stripMargin
    )
  }
}
