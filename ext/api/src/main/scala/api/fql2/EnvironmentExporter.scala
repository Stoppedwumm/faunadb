package fauna.api.fql2

import fauna.auth.Auth
import fauna.codex.json2.JSON
import fauna.model.runtime.fql2.{ FQLInterpreter, GlobalContext }
import fauna.repo.query.Query
import fql.ast.display._
import fql.typer.{ TypeScheme, Typer }
import fql.typer.TypeShape.TypeHint
import scala.collection.immutable.SortedMap

object EnvironmentExporter {
  import Environment._

  def exportEnvironment(auth: Auth): Query[Environment] = {
    GlobalContext.completeEnv(new FQLInterpreter(auth)).map { truncatedEnv =>
      val env = truncatedEnv.typeEnv
      val globals = env.globalTypes.view
        .mapValues(typeSchemeToDisplayString)
        .to(SortedMap)
      val typeShapes = env.typeShapes.view
        .mapValues { typeShape =>
          TypeShape(
            self = typeSchemeToDisplayString(typeShape.self),
            // fields that start with $ are an implementation detail of reification
            // we don't want to make these public and it is invalid syntax so
            // omitting here
            fields = typeShape.fields.view
              .collect {
                case (k, v) if !k.startsWith("$") =>
                  k -> typeSchemeToDisplayString(v)
              }
              .to(SortedMap),
            ops = typeShape.ops.view
              .collect {
                case (k, v) if !Typer.HardcodedOps.contains(k) =>
                  k -> typeSchemeToDisplayString(v)
              }
              .to(SortedMap),
            apply = typeShape.apply.map(typeSchemeToDisplayString(_)),
            access = typeShape.access.map(typeSchemeToDisplayString(_)),
            alias = typeShape.alias.map(typeSchemeToDisplayString(_)),
            typeHint = typeShape.typeHint.map {
              case TypeHint.ServerCollection => "server_collection"
              case TypeHint.UserCollection   => "user_collection"
              case TypeHint.Module           => "module"
            }
          )
        }
        .to(SortedMap)
      val truncatedResources = Seq.newBuilder[String]
      if (truncatedEnv.collectionsTruncated) {
        truncatedResources.addOne("collection")
      }
      if (truncatedEnv.functionsTruncated) {
        truncatedResources.addOne("function")
      }
      Environment(
        globals = globals,
        types = typeShapes,
        truncatedResources = truncatedResources.result()
      )
    }
  }

  private def typeSchemeToDisplayString(typeScheme: TypeScheme): String = {
    Typer().toTypeSchemeExpr(typeScheme).display
  }
}

object Environment {
  import EnvironmentResponse.Field
  object TypeShape {
    implicit val JSONCodec = new JSON.Encoder[TypeShape] {
      override def encode(stream: JSON.Out, opt: TypeShape): JSON.Out = {
        stream.writeObjectStart()
        stream.writeObjectField(Field.Self, JSON.encode(stream, opt.self))
        if (opt.fields.nonEmpty) {
          stream.writeObjectField(Field.Fields, JSON.encode(stream, opt.fields))
        }
        if (opt.ops.nonEmpty) {
          stream.writeObjectField(Field.Ops, JSON.encode(stream, opt.ops))
        }
        if (opt.apply.isDefined) {
          stream.writeObjectField(
            Field.Apply,
            JSON.encode(stream, opt.apply)
          )
        }
        if (opt.access.isDefined) {
          stream.writeObjectField(
            Field.Access,
            JSON.encode(stream, opt.access)
          )
        }
        if (opt.alias.isDefined) {
          stream.writeObjectField(
            Field.Alias,
            JSON.encode(stream, opt.alias)
          )
        }
        if (opt.typeHint.isDefined) {
          stream.writeObjectField(
            Field.TypeHint,
            JSON.encode(stream, opt.typeHint)
          )
        }
        stream.writeObjectEnd()
      }
    }
  }

  case class TypeShape(
    self: String,
    fields: SortedMap[String, String],
    ops: SortedMap[String, String],
    apply: Option[String],
    access: Option[String],
    alias: Option[String],
    typeHint: Option[String])

  implicit val JSONCodec = new JSON.Encoder[Environment] {
    override def encode(stream: JSON.Out, opt: Environment): JSON.Out = {
      stream.writeObjectStart()
      stream.writeObjectField(Field.Globals, JSON.encode(stream, opt.globals))
      stream.writeObjectField(Field.Types, JSON.encode(stream, opt.types))
      if (opt.truncatedResources.nonEmpty) {
        stream.writeObjectField(
          Field.TruncatedResources,
          JSON.encode(stream, opt.truncatedResources))
      }
      stream.writeObjectEnd()
    }
  }
}

case class Environment(
  globals: SortedMap[String, String],
  types: SortedMap[String, Environment.TypeShape],
  truncatedResources: Seq[String] = Seq.empty)
