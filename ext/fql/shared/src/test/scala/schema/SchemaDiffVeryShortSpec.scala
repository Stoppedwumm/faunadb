package fql.test.schema

class SchemaDiffVeryShortSpec extends SchemaDiffHelperSpec {
  "SchemaDiff" should "detect additions" in {
    diffVeryShort(before = "")(
      """|collection Foo {
         |}
         |
         |function foo() { 0 }
         |""".stripMargin
    )(
      """|* Adding collection `Foo` to main.fsl:1:1
         |* Adding function `foo` to main.fsl:4:1
         |""".stripMargin
    )
  }

  it should "not show individual item diffs" in {
    diffVeryShort(
      """|collection Foo {
         |  name: String
         |}
         |""".stripMargin
    )(
      """|collection Foo {
         |  name: String
         |
         |  index byName {
         |    terms [.name]
         |  }
         |}
         |""".stripMargin
    )(
      """|* Modifying collection `Foo` at main.fsl:1:1
         |""".stripMargin
    )
  }
}
