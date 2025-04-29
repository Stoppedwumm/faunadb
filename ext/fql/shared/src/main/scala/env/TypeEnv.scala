package fql.env

import fql.typer.TypeScheme
import fql.typer.TypeShape
import fql.typer.Typer

/** Stores a collection of globals and type shapes. This can represent the standard library,
  * or a fully typechecked database environment.
  */
final case class TypeEnv(
  val globalTypes: Map[String, TypeScheme],
  val typeShapes: Map[String, TypeShape]
) {
  def newTyper() =
    Typer(globalTypes, typeShapes)

  def ++(other: TypeEnv) =
    TypeEnv(globalTypes ++ other.globalTypes, typeShapes ++ other.typeShapes)
}
