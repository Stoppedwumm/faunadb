package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.lang.Timestamp
import fauna.model.Database
import fauna.repo.test.CassandraHelper
import scala.util.Random

trait ASTHelpers {
  val ctx = CassandraHelper.context("model")

  def newScope: ScopeID = newScope(RootAuth, Random.nextInt().toString, APIVersion.V20)

  def newScope(auth: Auth, name: String, vers: APIVersion): ScopeID = {
    val params = ObjectL("quote" -> ObjectL("name" -> StringL(name), "api_version" -> StringL(vers.toString)))
    run(auth, ObjectL("create" -> NativeRef("databases"), "params" -> params), TS(1)) match {
      case Right(VersionL(v, _)) => v.data(Database.ScopeField)
      case Right(r)              => sys.error(s"Unexpected: $r")
      case Left(errs)            => sys.error(errs mkString ", ")
    }
  }

  def run(auth: Auth, q: Literal, ts: Timestamp = Timestamp.MaxMicros, apiVersion: APIVersion = APIVersion.Default): Either[List[Error], Literal] =
    ctx ! EvalContext.write(auth, ts, apiVersion).parseAndEvalTopLevel(q)

  def parse(auth: Auth, q: Literal, apiVersion: APIVersion = APIVersion.Default): Either[List[Error], Expression] =
    ctx ! QueryParser.parse(auth, q, apiVersion)

  def parseEscapes(auth: Auth, q: Literal, apiVersion: APIVersion = APIVersion.Default): Either[List[Error], Literal] =
    (ctx ! EscapesParser.parse(auth, q, apiVersion)).toEither

  def TS(ts: Long) = Timestamp.ofMicros(ts)

  def Ref(str: String) = ObjectL("@ref" -> StringL(str))

  def NativeRef(str: String) = ObjectL("@ref" -> ObjectL("id" -> StringL(str)))

  def Pos(elems: Position.PosElem*) = Position(elems: _*)

  def funcall(name: String, args: Literal*) =
    ObjectL(name -> ArrayL(args.toList))

  def varref(name: String) =
    ObjectL("var" -> StringL(name))

  def lambda(varname: String, expr: Literal) =
    ObjectL("lambda" -> StringL(varname), "expr" -> expr)

  def map(lambda: Literal, coll: Literal) =
    ObjectL("map" -> lambda, "collection" -> coll)

  def let(bindings: ObjectL, inExpr: Literal) =
    ObjectL("let" -> bindings, "in" -> inExpr)

  def let(bindings: ArrayL, inExpr: Literal) =
    ObjectL("let" -> bindings, "in" -> inExpr)

  def select(path: Literal, from: Literal, default: Literal) =
    ObjectL("select" -> path, "from" -> from, "default" -> default)
}
