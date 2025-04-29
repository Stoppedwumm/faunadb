package fauna.model.test

import fauna.model.schema.fsl.SourceFile
import fql.ast.{ Span, Src }

class SourceFileSpec extends Spec {

  "FSL" - {
    "can parse fsl file" in {
      val file =
        SourceFile.FSL(
          SourceFile.Builtin.Main.filename,
          "collection Foo {}"
        )

      val items = file.parse() getOr { errs => fail(errs.toString) }
      items should have size 1
    }

    "enforces correct extension" in {
      val err =
        the[IllegalArgumentException] thrownBy SourceFile.FSL(
          "foo.bar",
          "collection Foo {}"
        )

      err.getMessage should include("invalid filename foo.bar")
    }

    "appends an fsl snippet" in {
      val file =
        SourceFile.FSL(
          SourceFile.Builtin.Main.filename,
          "// empty file"
        )

      val newFile = file.append("// another line")

      newFile.content shouldEqual
        s"""|// empty file
            |
            |// another line""".stripMargin
    }

    "patches an fsl file" in {
      val file =
        SourceFile.FSL(
          SourceFile.Builtin.Main.filename,
          s"""|function double(x) { x * 2 }
              |function triple(x) { x * 3 }
              |function square(x) { x * x }
              |""".stripMargin
        )

      val newFile =
        file.patch(
          Span(29, 57, Src.Null),
          "function id(x) { x }"
        )

      newFile.content shouldEqual
        s"""|function double(x) { x * 2 }
            |function id(x) { x }
            |function square(x) { x * x }
            |""".stripMargin
    }
  }
}
