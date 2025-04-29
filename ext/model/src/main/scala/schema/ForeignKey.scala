package fauna.model.schema

import fauna.atoms.{
  AccessProviderID,
  DocID,
  ID,
  IDCompanion,
  KeyID,
  RoleID,
  ScopeID,
  UserFunctionID
}
import fauna.lang.syntax._
import fauna.model.{ RuntimeEnv, SchemaNames }
import fauna.repo.query.Query
import fauna.repo.schema.Path
import fauna.storage.doc.Diff
import fauna.storage.ir.{ ArrayV, DocIDV, IRValue, MapV }

object ForeignKey {
  // TODO: Replace full-table scan with indexed lookups of FKs.
  sealed abstract class Location[A <: ID[A]](
    override val toString: String,
    val idComp: IDCompanion[A]
  ) {
    def refersTo(
      scope: ScopeID,
      path: List[String],
      docID: DocID,
      value: IRValue
    ): Query[Seq[String]] = {
      RuntimeEnv.Static
        .Store(scope)
        .allDocs(idComp.collID)
        .flatMapValuesT { vs =>
          val fields = vs.data.fields
          val checks = traverse(path, fields) {
            case (_, `value`) => true
            // this is here because the database field on Keys are the DocID
            // for the database.
            // this prevents us from deleting a database that still have keys
            // that reference it.
            case (_, DocIDV(`docID`)) => true
          }

          val pass = checks.foldLeft(false) { _ || _ }

          if (pass) {
            val name = vs.data
              .getOrElse(
                SchemaNames.NameField,
                SchemaNames.Name(vs.docID.subID.toLong.toString))
              .toString
            Query.value(Seq(name))
          } else {
            Query.value(Nil)
          }
        }
        .flattenT
    }

    // TODO: Replace full-table scan with indexed lookups of FKs.
    def rename(
      scope: ScopeID,
      path: List[String],
      prevVal: IRValue,
      newVal: IRValue
    ): Query[Unit] = {
      RuntimeEnv.Static.Store(scope).allDocs(idComp.collID).foreachValueT { vs =>
        traverseAndUpdate(path, vs.data.fields, prevVal, newVal) match {
          case None => Query.unit
          case Some(updatedData) =>
            RuntimeEnv.Static
              .Store(scope)
              .internalUpdate(vs.docID, Diff(updatedData))
              .join
        }
      }
    }

    private def traverse[A](path: List[String], value: IRValue)(
      pf: PartialFunction[(Path, IRValue), A]): Seq[A] = {

      def traverse0(
        path: List[String],
        value: IRValue,
        prefix: Path.Prefix): Seq[A] = {

        (path, value) match {
          case (p :: ps, map @ MapV(_)) =>
            map
              .get(p :: Nil)
              .map(traverse0(ps, _, prefix :+ p))
              .getOrElse(Seq.empty)

          case (ps, ArrayV(elems)) =>
            elems.zipWithIndex flatMap { case (ir, idx) =>
              traverse0(ps, ir, prefix :+ idx)
            }

          case (Nil, ir) =>
            (prefix.toPath, ir) match {
              case pf(res) => Seq(res)
              case _       => Seq.empty
            }

          case (_, _) =>
            Seq.empty
        }
      }

      traverse0(path, value, Path.RootPrefix)
    }

    /** Traverses the path in the data IRValue and if a value is found at path that matches prevVal, it will update
      * the value at path to be newVal.
      * If nothing at path is found or if the value at path does not match prevVal, it will return the original data.
      * If while traversing path it encounters an array it will traverse every element in the array.
      */
    private def traverseAndUpdate(
      path: List[String],
      data: MapV,
      prevVal: IRValue,
      newVal: IRValue): Option[MapV] = {
      def traverseAndUpdate0(
        path: List[String],
        data: IRValue,
        prevVal: IRValue,
        newVal: IRValue
      ): IRValue = {
        (path, data) match {
          case (field :: ps, map @ MapV(_)) if map.contains(List(field)) =>
            map.update(
              List(field),
              traverseAndUpdate0(ps, map.get(List(field)).get, prevVal, newVal))

          case (ps, ArrayV(elems)) =>
            val newElems = elems.map { ir =>
              traverseAndUpdate0(ps, ir, prevVal, newVal)
            }
            ArrayV(newElems)

          case (Nil, ir) if ir == prevVal =>
            newVal

          case (_, _) =>
            data
        }
      }

      val updatedData = traverseAndUpdate0(path, data, prevVal, newVal) match {
        case v: MapV => v
        case v =>
          throw new IllegalStateException(
            s"Unexpected data transformation, expected a MapV to be returned, received ${v.vtype}"
          )
      }

      Option.when(updatedData != data) {
        updatedData
      }
    }
  }

  object Location {
    case object UserFunction extends Location("function", UserFunctionID)
    case object Role extends Location("role", RoleID)
    case object AccessProvider extends Location("access provider", AccessProviderID)
    case object Key extends Location("key", KeyID)
  }

  final class Target private (
    val loc: Location[_],
    val path: List[String],
    val revalidate: Boolean)
  object Target {
    def apply(loc: Location[_], revalidate: Boolean = true)(
      path: String,
      paths: String*) =
      new Target(loc, path :: paths.toList, revalidate)
  }

  final class Src private (val path: List[String], val targets: Seq[Target]) {
    def forRevalidation: Option[Src] = {
      val revalidateTargets = targets.filter(_.revalidate)
      Option.when(revalidateTargets.nonEmpty) {
        new Src(path, revalidateTargets)
      }
    }
  }
  object Src {
    def apply(path: String, paths: String*)(target: Target, targets: Target*) =
      new Src(path :: paths.toList, target +: targets.toSeq)
  }
}
