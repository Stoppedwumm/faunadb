package fql.test.error

import fql.parser._

class SchemaErrorSpec extends ErrorSpec {

  def parse(query: String) = Parser.schemaItems(query)

  "Schema errors" should "look nice" in {
    // NOTE: The second and third arguments can be ommited from `error`, and
    // that will cause the error message to just be printed to the console, for
    // debugging.

    error("2", "", "Expected end-of-input")
    error("foo", "foo", "Invalid schema item `foo`")

    error("collection", "collection", "Missing required name")
    error("collection #", "#", "Expected end-of-input or identifier")
    error("collection {}", "collection", "Missing required name")
    error("collection foo { bar }", "bar", "Invalid field or member `bar`")
    error("collection foo { index bar { baz } }", "baz", "Invalid member `baz`")
    error("collection foo { unique }", "unique", "Missing required value")
    error("collection foo { unique [bar] }", "bar", "Invalid field path")

    error("function", EOF, "Expected name")
    error("function #", "#", "Expected name")
    error("function foo {}", 13, 14, "Expected `(`")
    error(
      "function foo() {}",
      "}",
      "Unexpected end of block. Expected statement or expression")
    error("function foo(x: y: z) { x }", 17, 18, "Expected `)`, `,`, or `<`")

    error("role", "role", "Missing required name")
    error("role #", "#", "Expected end-of-input or identifier")
    error("role {}", "role", "Missing required name")
    error("role MyRole { foo }", "foo", "Invalid member `foo`")
    error("role MyRole { privileges {} }", "privileges", "Missing required name")

    error("access provider", EOF, "Expected name")
    error("access provider #", "#", "Expected name")
    error(
      "access provider MyAP { issuer ''; jwks_uri ''; foo }",
      "foo",
      "Invalid member `foo`")
    error(
      "access provider MyAP { issuer; jwks_uri 'foo' }",
      "issuer;",
      "Missing required value")
    error("access provider MyAP { issuer 'foo' }", 21, 37, "Missing jwks uri")
  }

  it should "not break" in {
    error("collection foo { unique [bar] }", "bar", "Invalid field path")
  }
}
