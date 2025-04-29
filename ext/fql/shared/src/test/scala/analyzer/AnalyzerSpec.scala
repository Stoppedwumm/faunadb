package fql.test

import fql.parser.Parser
import fql.typer.{ TypeScheme, TypeShape, Typer }

trait AnalyzerSpec extends Spec {
  val globals = Map(
    "Time" -> ts("TimeModule"),
    "Math" -> ts("MathModule"),
    "desc" -> ts("<A> (A => Any) => { desc: A => Any }"),
    "String" -> ts("StringModule"),
    "Set" -> ts("SetModule"),
    "Function" -> ts("FunctionCollection"),
    "timeOrStr" -> ts("Time | String"),
    "Collection" -> ts("CollectionCollection"),
    "User" -> ts("UserCollection")
  )
  val shapes = Map(
    "TimeModule" -> TypeShape(
      self = ts("TimeModule"),
      fields = Map(
        "now" -> ts("() => Time"),
        "fromString" -> ts("(String) => Time")
      )
    ),
    "Time" -> TypeShape(
      self = ts("Time"),
      fields = Map(
        "year" -> ts("Number"),
        "month" -> ts("Number"),
        "toString" -> ts("() => String"),
        "add" -> ts("(Number, String) => Time")
      )
    ),
    "MathModule" -> TypeShape(
      self = ts("MathModule"),
      fields = Map(
        "gimmeIntOrStrStr" -> ts("(Number => Null) & ((String, String) => Null)")
      )
    ),
    "String" -> TypeShape(
      self = ts("String"),
      fields = Map(
        "toString" -> ts("() => String"),
        "parseInt" -> ts("() => Number | Null")
      )
    ),
    "Set" -> TypeShape(
      self = ts("<A> Set<A>"),
      fields = Map(
        "fold" -> ts("<A, B> (seed: B, reducer: (B, A) => B) => B")
      )
    ),
    "Array" -> TypeShape(
      self = ts("<A> Array<A>"),
      fields = Map(
        "take" -> ts("<A> (amount: Number) => Array<A>"),
        "indexOf" -> ts("<A> (A => Number | Null) & ((A, Number) => Number | Null)")
      )
    ),
    "Number" -> TypeShape(
      self = ts("Number"),
      fields = Map(
        "toString" -> ts("() => String")
      )
    ),
    "Boolean" -> TypeShape(
      self = ts("Boolean"),
      fields = Map(
        "toString" -> ts("() => String")
      )
    ),
    "CollectionCollection" -> TypeShape(
      self = ts("CollectionCollection"),
      fields = Map(
        "all" -> ts(
          "(() => Set<CollectionDef>) & ({ from: Any } | { to: Any } | { to: Any, from: Any } => Set<CollectionDef>)"),
        "firstWhere" -> ts(
          "(CollectionDef => Boolean) => CollectionDef | NullCollectionDef"),
        "byName" -> ts("String => CollectionDef | NullCollectionDef"),
        "toString" -> ts("() => String"),
        "where" -> ts("(CollectionDef => Boolean) => Set<CollectionDef>"),
        "createData" -> ts("""|{
                          |  name: String,
                          |  indexes: {
                          |    *: {
                          |      queryable: Boolean | Null,
                          |      values: Array<{ order: "asc" | "desc" | Null, field: String, mva: Boolean | Null }> | Null,
                          |      terms: Array<{ field: String, mva: Boolean | Null }> | Null
                          |    }
                          |  } | Null,
                          |  data: { *: Any } | Null,
                          |  alias: String | Null,
                          |  constraints: Array<{ unique: Array<String> }> | Null,
                          |  history_days: Number | Null
                          |} => CollectionDef""".stripMargin),
        "create" -> ts("""|{
                          |  name: String,
                          |  indexes: {
                          |    *: {
                          |      queryable: Boolean | Null,
                          |      values: Array<{ order: "asc" | "desc" | Null, field: String, mva: Boolean | Null }> | Null,
                          |      terms: Array<{ field: String, mva: Boolean | Null }> | Null
                          |    }
                          |  } | Null,
                          |  data: { *: Any } | Null,
                          |  alias: String | Null,
                          |  constraints: Array<{ unique: Array<String> }> | Null,
                          |  history_days: Number | Null
                          |} => CollectionDef""".stripMargin)
      )
    ),
    "FunctionCollection" -> TypeShape(
      self = ts("FunctionCollection"),
      fields = Map(
        "create" -> ts("""|(data: {
                          |  body: String,
                          |  name: String,
                          |  role: String | Null,
                          |  data: { *: Any } | Null,
                          |  signature: String | Null,
                          |  alias: String | Null
                          |}) => FunctionDef""".stripMargin)
      )
    ),
    "UserCollection" -> TypeShape(
      self = ts("UserCollection"),
      fields = Map(
        "byId" -> ts("String => User | Null"),
        "create" -> ts("""|{
                          |  name: String,
                          |  nested: { foo: Int },
                          |} => User""".stripMargin)
      )
    ),
    "User" -> TypeShape(
      self = ts("UserCollection"),
      fields = Map(
        "exists" -> ts("() => Boolean")
      ),
      alias = Some(ts("{ my_computed_field: Int }"))
    )
  )

  def ts(str: String): TypeScheme = {
    Parser.typeSchemeExpr(str, fql.ast.Src.Null) match {
      case fql.Result.Ok(sch) =>
        Typer.typeTSchemeUnchecked(sch)
      case fql.Result.Err(err) => fail(s"invalid scheme $str: $err")
    }
  }
}
