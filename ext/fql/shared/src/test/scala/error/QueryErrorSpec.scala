package fql.test.error

import fql.parser._

class QueryErrorSpec extends ErrorSpec {

  def parse(query: String) = Parser.query(query)

  "Query errors" should "look nice" in {
    // NOTE: The second and third arguments can be ommited from `error`, and
    // that will cause the error message to just be printed to the console, for
    // debugging.

    error("", "", "Unexpected end of query. Expected statement or expression")
    error("2 3", "3", "Expected end-of-input")

    error("{", EOF, "Unexpected end of block. Expected statement or expression")
    error("{ a:", EOF, "Expected expression")
    error("{ a: }", "}", "Expected expression")
    error("{ a: (3 }", "}", "Expected `)`, `,`, or `...`")
    error("{ a: [3 }", "}", "Expected `,` or `]`")

    error("if", EOF, "Expected `(`")
    error("if true", "t", "Expected `(`")
    error("if true 3 else", "t", "Expected `(`")
    error("if (true)", EOF, "Expected expression")
    error("if (true) 3 else", EOF, "Expected expression")

    error("##", "#", "Unexpected end of query. Expected statement or expression")

    error("let", EOF, "Expected identifier")
    error("let.foo", ".", "Expected identifier")

    error("let a", EOF, "Expected `:` or `=`")
    error("let a:", EOF, "Expected type")
    error("let a: int", EOF, "Expected `<` or `=`")
    error("let a =", EOF, "Expected expression")
    error("let a: int =", EOF, "Expected expression")

    error("3.", EOF, "Expected digit or identifier")
    error("foo(,)", ",", "Expected `)` or expression")
    error("foo(", EOF, "Expected `)` or expression")
    error("foo(a", EOF, "Expected `)` or `,`")
    error("foo(3", EOF, "Expected `)` or `,`")

    error("'\\u12'", 5, 6, "Expected hex digit")
    error("'\\uz'", "z", "Expected `{` or hex digit")
    error("'\\u{1234'", 8, 9, "Expected `}` or hex digit")
    error("'\\3'", "3", "Expected escape code")

    // projection
    error("foo { 3 }", "3", "Expected `*` or identifier")
    error("foo { foo: }", "}", "Expected expression")
    error("foo { foo: 3 bar }", "b", "Expected `,` or `}`")
  }
}
