package fql.env

import fql.ast.{ Name, Span, TypeExpr }
import fql.ast.display._
import fql.typer._
import scala.collection.immutable.ArraySeq

/** The result of typechecking a database.
  */
class TypecheckedEnvironment(
  val stdlib: TypeEnv,
  val collections: Map[String, CollectionTypeInfo.Checked],
  val functions: Map[String, TypeExpr.Scheme]) {

  /** Returns a list of internal updates to apply after the environment has been typechecked.
    *
    * This is used by model to fill in the internal signatures of functions and collections.
    */
  def updates: Iterable[InternalUpdate] = {
    val collUpdates = collections.view.map { case (name, coll) =>
      CollectionUpdate(name, coll.computedFieldsExpr.map(_.display))
    }

    val funcUpdates = functions.view.map { case (name, func) =>
      FunctionUpdate(name, func.display)
    }

    collUpdates ++ funcUpdates
  }

  /** This is the resulting static environment, that can be used at runtime to
    * typecheck queries. In practice its only used in the migration validator, and by
    * fql-analyzer. Queries lookup the environment from model instead.
    */
  private def userEnvironment = {
    val globals = Map.newBuilder[String, TypeScheme]
    collections.values.foreach { coll =>
      globals += coll.docName.str -> coll.collType.typescheme

      coll.aliased.foreach { aliased =>
        globals += aliased.docName.str -> aliased.collType.typescheme
      }
    }
    functions.foreach { case (name, func) =>
      val sig = Typer.typeTSchemeUncheckedType(func)
      globals += name -> Type
        .Named(Name("UserFunction", Span.Null), Span.Null, ArraySeq(sig))
        .typescheme
    }

    val shapes = Map.newBuilder[String, TypeShape]
    collections.values.foreach { coll =>
      shapes ++= coll.allShapes

      coll.aliased.foreach { aliased =>
        shapes ++= aliased.allShapes
      }
    }

    TypeEnv(globals.result(), shapes.result())
  }

  def environment = stdlib ++ userEnvironment
}

sealed trait InternalUpdate

final case class CollectionUpdate(name: String, computedFieldsExpr: Option[String])
    extends InternalUpdate

final case class FunctionUpdate(name: String, internalSig: String)
    extends InternalUpdate
