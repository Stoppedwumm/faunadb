package fql.test.schema

import fql.ast._
import fql.color.ColorKind
import fql.parser.Parser
import fql.schema.{ DiffRender, SchemaDiff }
import fql.Result
import fql.TextUtil
import org.scalactic.source.Position

class SchemaDiffColorSpec extends SchemaDiffHelperSpec {

  val reset = "\u001b[0m"
  val bold = "\u001b[1m"

  // Red, green and yellow. These are one letter so that the tests are somewhat
  // readable.
  val r = "\u001b[31m"
  val g = "\u001b[32m"
  val y = "\u001b[33m"
  val b = "\u001b[34m"

  val boldRed = "\u001b[1;31m"
  val boldGreen = "\u001b[1;32m"
  val boldYellow = "\u001b[1;33m"
  val boldBlue = "\u001b[1;34m"

  "SchemaDiff" should "render colors" in {
    diff("")(
      """|collection Foo {
         |}
         |""".stripMargin
    )(
      s"""|${boldBlue}* Adding collection `Foo`${reset} to main.fsl:1:1
          |
          |""".stripMargin
    )

    diff(
      """|collection Foo {
         |}
         |""".stripMargin
    )("")(
      s"""|${boldBlue}* Removing collection `Foo`${reset} from main.fsl:1:1
          |
          |""".stripMargin
    )

    diff(
      "collection Foo {}",
      Src.SourceFile("main.fsl")
    )(
      """|collection Foo {
         |  history_days 10
         |}
         |""".stripMargin,
      Src.SourceFile("collections.fsl")
    )(
      s"""|${boldBlue}* Modifying collection `Foo`${reset} at collections.fsl:1:1 (previously defined at main.fsl:1:1):
          |  * Configuration:
          |$g  + add history_days set to 10${reset}
          |
          |""".stripMargin
    )
  }

  it should "render migrations in color" in {
    diffMigrations(
      """|collection Foo {
         |  migrations {
         |    add .foo
         |    drop .foo
         |  }
         |}""".stripMargin
    )(
      """|collection Foo {
         |  a: Int
         |  b: String = "hello"
         |  c: String?
         |  d: String
         |  dump: { *: Any }?
         |
         |  migrations {
         |    add .foo
         |    drop .foo
         |    add .bye
         |    drop .bye
         |    add .a
         |    backfill .a = 11
         |    add .b
         |    add .c
         |    split .c -> .c, .d
         |    backfill .d = "split!"
         |    add .dump
         |    move_conflicts .dump
         |    move_wildcard .dump
         |  }
         |}""".stripMargin
    )(
      s"""|${boldBlue}* Modifying collection `Foo`${reset} at main.fsl:1:1:
          |  * Defined fields:
          |$r  - drop field `.bye`${reset}
          |$g  + add field `.a` with backfill `11`${reset}
          |$g  + add field `.b` with backfill `"hello"`${reset}
          |$y  ~ split field `.c` into fields `.c`,`.d`${reset}
          |    - if the value matches type `String | Null`, the value stays in field `.c`
          |    - else if the value matches type `String`, the value moves to field `.d`
          |$y  + backfill field `.d` with value `"split!"`${reset}
          |$y  ~ move values with type conflicts into `.dump`${reset}
          |$y  ~ move all fields into the field `.dump` except fields in the list `[.d, .dump, .c, .b, .a]`${reset}
          |
          |""".stripMargin
    )
  }

  it should "detect multiple changes" in {
    diffMigrations(
      """|collection Foo {
         |  history_days 10
         |  index byBar {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  check ok ((doc) => doc.ok)
         |  check cool ((doc) => doc.colo)
         |}
         |
         |function double(n) {
         |  2 * n
         |}
         |""".stripMargin
    )(
      """|@alias(Bar)
         |collection Foo {
         |  compute floof: Int = ((doc) => !doc.notfloof)
         |  index byBuz {
         |    terms [.bar]
         |    values [.bar, .baz]
         |  }
         |  unique [mva(.bar)]
         |  check notok ((doc) => !doc.ok)
         |  check cool ((doc) => doc.cool)
         |  history_days 0
         |  ttl_days 10
         |}
         |
         |function double(a) {
         |  a * 2
         |}
         |""".stripMargin
    )(
      s"""|${boldBlue}* Modifying collection `Foo`${reset} at main.fsl:1:1:
          |  * Computed fields:
          |$g  + add computed field `floof`${reset}
          |
          |  * Indexes:
          |$g  + add index `byBuz`${reset}
          |$r  - remove index `byBar`${reset}
          |
          |  * Constraints:
          |$g  + add unique constraint on [mva(.bar)]${reset}
          |$g  + add check constraint `notok`${reset}
          |$r  - remove check constraint `ok`${reset}
          |$y  ~ change check constraint `cool` body${reset}
          |
          |  * Configuration:
          |$g  + add alias set to Bar${reset}
          |$g  + add ttl_days set to 10${reset}
          |$y  ~ change history_days from 10 to 0${reset}
          |
          |${boldBlue}* Modifying function `double`${reset} at main.fsl:15:1:
          |$y  ~ change signature${reset}
          |$y  ~ change body${reset}
          |
          |""".stripMargin
    )
  }

  private def diff(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) = {
    val beforeItems = Parser.schemaItems(before, srcBefore) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val afterItems = Parser.schemaItems(after, srcAfter) match {
      case Result.Ok(v)  => v
      case Result.Err(e) => fail(s"failed to parse: $e")
    }
    val diffs = SchemaDiff.diffItems(beforeItems, afterItems, renames)

    val diffText = DiffRender.renderSemantic(
      Map(srcBefore -> before),
      Map(srcAfter -> after),
      diffs,
      ColorKind.Ansi
    )

    if (diffText != expectedDiff) {
      withClue(TextUtil.diff(diffText, expectedDiff)) {
        fail("generated and expected diffs do not match")
      }
    }
  }

  private def diffMigrations(
    before: String,
    srcBefore: Src.Id = Src.SourceFile("main.fsl")
  )(
    after: String,
    srcAfter: Src.Id = Src.SourceFile("main.fsl"),
    renames: SchemaDiff.Renames = Map.empty
  )(
    expectedDiff: String
  )(implicit pos: Position) = {
    diffMigrations0(before, srcBefore)(after, srcAfter, renames)(expectedDiff)(
      DiffRender.renderSemantic(_, _, _, ColorKind.Ansi))
  }
}
