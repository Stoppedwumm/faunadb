package fauna.model.runtime.fql2.stdlib

import fauna.auth.{ AdminPermissions, JWTLogin, LoginAuth, TokenLogin }
import fauna.codex.json._
import fauna.model.runtime.fql2.{
  FQLInterpreter,
  ModuleObject,
  QueryRuntimeFailure,
  ValueOps
}
import fauna.model.Database
import fauna.repo.values.Value
import fql.ast.Span

object Query extends ModuleObject("Query") {

  defStaticFunction("identity" -> tt.Optional(tt.AnyDoc))() { (ctx) =>
    ctx.auth match {
      case LoginAuth.Identity(id) =>
        Value.Doc(id).toQuery
      case _ =>
        missingIdentity(ctx.stackTrace.currentStackFrame).toQuery
    }
  }

  defStaticFunction("token" -> tt.Optional(tt.Union(tt.AnyDoc, tt.AnyStruct)))() {
    (ctx) =>
      ctx.auth match {
        case LoginAuth.Source(JWTLogin(jwt, _)) =>
          toValue(jwt.payload, ctx.stackTrace.currentStackFrame).toQuery

        case LoginAuth.Source(TokenLogin(token)) =>
          Value.Doc(token.id.toDocID).toQuery

        case _ =>
          Value.Null
            .noSuchElement("no token", ctx.stackTrace.currentStackFrame)
            .toQuery
      }
  }

  defStaticFunction("isEnvTypechecked" -> tt.Boolean)() { (ctx) =>
    FQLInterpreter.isEnvTypechecked(ctx.scopeID).map(Value.Boolean(_).toResult)
  }

  defStaticFunction("isEnvProtected" -> tt.Boolean)() { (ctx) =>
    ctx.auth.permissions match {
      case AdminPermissions =>
        Database.isProtected(ctx.scopeID).map(Value.Boolean(_).toResult)
      case _ => QueryRuntimeFailure.PermissionDenied(ctx.stackTrace).toQuery
    }
  }

  private def toValue(jsValue: JSValue, span: Span): Value = jsValue match {
    case JSNull          => Value.Null(span)
    case JSTrue          => Value.True
    case JSFalse         => Value.False
    case JSLong(value)   => Value.Long(value)
    case JSDouble(value) => Value.Double(value)
    case JSString(value) => Value.Str(value)
    case array: JSArray =>
      Value.Array.fromSpecific(array.value.map(toValue(_, span)))
    case obj: JSObject =>
      Value.Struct.fromSpecific(obj.value map { case (k, v) =>
        k -> toValue(v, span)
      })
    // other types should not be valid on the wire
    case _ => Value.Null(span)
  }

  private def missingIdentity(span: Span) =
    Value.Null.noSuchElement("missing identity", span)
}
