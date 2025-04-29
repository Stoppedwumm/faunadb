package fauna.model.test

import fauna.ast._
import fauna.atoms.APIVersion
import fauna.auth.{ Auth, RootAuth }
import fauna.lang.syntax._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import scala.annotation.unused

object FooFunction extends QFunction {
  val effect = Effect.Pure

  def apply(foo: Literal, @unused ec: EvalContext, @unused pos: Position): Query[R[Literal]] = {
    Query.value(Right(foo))
  }
}

object SpecParser {
  val forms = MapRouter.build[Literal, Query[PResult[Expression]], (Auth, Position)] { forms =>
    forms.add("foo_form_all_versions") {
      case (foo, _, _) => PResult.successfulQ(foo)
    }

    forms.add("foo_form_introduced_21", APIVersion.introducedOn(APIVersion.V21)) {
      case (foo, _, _) => PResult.successfulQ(foo)
    }

    forms.add("foo_form_removed_27", APIVersion.removedOn(APIVersion.V27)) {
      case (foo, _, _) => PResult.successfulQ(foo)
    }

    forms.addFunction("foo_func_all_versions" -> Casts.Any, FooFunction)
    forms.addFunction("foo_func_introduced_21" -> Casts.Any, FooFunction, APIVersion.introducedOn(APIVersion.V21))
    forms.addFunction("foo_func_removed_27" -> Casts.Any, FooFunction, APIVersion.removedOn(APIVersion.V27))
  }

  def parseExpr(
    auth: Auth,
    input: Literal,
    apiVersion: APIVersion,
    pos: Position): Query[PResult[Expression]] = {
    QueryParser.parse(auth, input, apiVersion, pos) flatMap {
      case Right(expr)  => PResult.successfulQ(expr)
      case Left(errors) => PResult.failedQ(errors)
    }
  }

  def parseOpt(
    auth: Auth,
    input: Option[Literal],
    apiVersion: APIVersion,
    pos: Position): Query[PResult[Option[Expression]]] = input match {
    case Some(inp) => parseExpr(auth, inp, apiVersion, pos) mapT { Some(_) }
    case None      => PResult.successfulQ(None)
  }
}

class MapRouterSpec extends Spec with ASTHelpers {
  val AllVersions = Set(APIVersion.V20, APIVersion.V21, APIVersion.V27)

  def isAvailable(apiVersion: APIVersion, name: String) = {
    val ctx = (RootAuth, RootPosition)
    SpecParser.forms(List(name -> NullL), ctx, apiVersion).isDefined
  }

  def availableVersions(name: String) =
    AllVersions filter { isAvailable(_, name) }

  "MapRouter" - {
    "non existent" in {
      availableVersions("foo") shouldBe Set.empty
    }

    "all versions" in {
      availableVersions("foo_form_all_versions") shouldBe AllVersions
      availableVersions("foo_func_all_versions") shouldBe AllVersions
    }

    "introduced on" in {
      availableVersions("foo_form_introduced_21") shouldBe Set(APIVersion.V21, APIVersion.V27)
      availableVersions("foo_func_introduced_21") shouldBe Set(APIVersion.V21, APIVersion.V27)
    }

    "removed on" in {
      availableVersions("foo_form_removed_27") shouldBe Set(APIVersion.V20, APIVersion.V21)
      availableVersions("foo_func_removed_27") shouldBe Set(APIVersion.V20, APIVersion.V21)
    }
  }
}
