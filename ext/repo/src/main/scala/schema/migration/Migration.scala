package fauna.repo.schema.migration

import fauna.repo.schema.SchemaType
import fauna.storage.doc.{ ConcretePath, Data }
import fauna.storage.ir._
import java.util.{ HashSet => JHashSet }
import scala.collection.mutable.{ Map => MMap }
import scala.util.Sorting

sealed trait Migration {
  def migrate(data: Data): Data
}

object Migration {
  val ConflictsPath = ConcretePath("__conflicts")

  /** Adds the value `value` at `path`.
    *
    * If there is already a value at `path`, then one of two things happen:
    * - If the value matches the type `discriminator`, nothing is changed. The
    *   existing value is left at `path`.
    * - If the value does not match discriminator`, it is moved to ConflictsPath,
    *   and the `value` is inserted at `path`.
    *
    * There will only ever be conflicts if there is a wildcard in the starting
    * schema. If there are conflicts, this migration _must_ be followed by a
    * `MoveConflicts` migration in the same schema version, as ConflictsPath cannot
    * be stored to disk.
    */
  final case class AddField(
    path: ConcretePath,
    discriminator: SchemaType,
    value: IRValue)
      extends Migration {
    def migrate(data0: Data): Data = {
      var data = data0

      (1 until path.path.length).foreach { i =>
        val prefix = ConcretePath(path.path.slice(0, i))
        if (data.get(prefix).isEmpty) {
          data = data.insert(prefix, MapV.empty)
        }
      }

      data.get(path) match {
        case Some(prev) =>
          if (SchemaType.isValueOfType(discriminator, prev)) {
            // The existing value already matches, so return the value with the
            // parent field added.
            return data
          } else {
            data = data.remove(path)

            val pathWithoutData = if (path.path.headOption == Some("data")) {
              ConcretePath(path.path.tail)
            } else {
              path
            }

            if (data.get(ConflictsPath).isEmpty) {
              data = data.insert(ConflictsPath, MapV.empty)
            }

            data = data.insert(ConflictsPath.concat(pathWithoutData), prev)
          }

        case None => ()
      }

      if (value != NullV) {
        data.insert(path, value)
      } else {
        data
      }
    }
  }

  /** Removes the field at `path`. Does nothing if `path` does not exist.
    */
  final case class DropField(path: ConcretePath) extends Migration {
    def migrate(data: Data): Data = data.remove(path)
  }

  /** Moves the field from `from` to `to`.
    *
    * Throws an illegal state exception if `to` already exists, regardless of
    * `from` being present or not.
    */
  final case class MoveField(from: ConcretePath, to: ConcretePath)
      extends Migration {
    def migrate(data: Data): Data = {
      // Check this regardless of the actual value in `data`.
      if (data.get(to).isDefined) {
        throw new IllegalStateException(
          s"Refusing to overwrite existing field at $to")
      }

      data.get(from) match {
        case Some(v) => data.remove(from).insert(to, v)
        case None    => data
      }
    }
  }

  /** This moves the conflicting fields from previous `AddField` migrations into
    * the `into` path.
    *
    * Specifically, `AddField` will add fields in the ConflictsField, and this
    * will move those fields into a user-defined `into` field.
    */
  final case class MoveConflictingFields(into: ConcretePath) extends Migration {
    def migrate(data: Data): Data = {
      var data0 = data

      data.get(ConflictsPath) match {
        case Some(conflicts: MapV) =>
          if (conflicts.elems.nonEmpty) {
            data.get(into) match {
              case None          => data0 = data0.insert(into, MapV.empty)
              case Some(MapV(_)) => ()
              case Some(_) =>
                throw new IllegalStateException(
                  s"Refusing to overwrite existing non-struct at $into")
            }
          }

          // Merge them in the order of the conflicts, which is the order of the
          // `add` migrations.
          conflicts.elems.foreach { case (name, value) =>
            var fieldName = name
            def intoPath = into.append(fieldName)

            while (data0.get(intoPath).isDefined) {
              fieldName = s"_${fieldName}"
            }

            data0 = data0.insert(intoPath, value)
          }

          data0 = data0.remove(ConflictsPath)

        // No changes needed.
        case _ => ()
      }

      data0
    }
  }

  /** This represents a `split` migration, which splits a field into another field.
    * 1. If the value is null (the field is unset), both fields are backfilled.
    * 2. - If the value matches `discriminator`:
    *      - The value in `field` is left alone.
    *      - `spillField` is filled in with `spillBackfill`.
    *    - If the value doesn't match `expected`:
    *      - The value in `field` is moved to `spillField`.
    *      - `field` is filled in with `fieldBackfill`.
    */
  final case class SplitField(
    field: ConcretePath,
    spillField: ConcretePath,
    discriminator: SchemaType,
    fieldBackfill: IRValue,
    spillBackfill: IRValue)
      extends Migration {
    def migrate(data: Data): Data = {
      if (data.get(spillField).isDefined) {
        throw new IllegalStateException(
          s"Refusing to overwrite existing field at $spillField")
      }

      val prev = data.get(field).getOrElse(NullV)
      if (prev == NullV) {
        val data0 = if (fieldBackfill == NullV) {
          data
        } else {
          data.insert(field, fieldBackfill)
        }
        if (spillBackfill == NullV) {
          data0
        } else {
          data0.insert(spillField, spillBackfill)
        }
      } else {
        if (SchemaType.isValueOfType(discriminator, prev)) {
          if (spillBackfill == NullV) {
            data
          } else {
            data.insert(spillField, spillBackfill)
          }
        } else {
          val removed0 = data.remove(field)
          val removed = if (fieldBackfill == NullV) {
            removed0
          } else {
            removed0.insert(field, fieldBackfill)
          }
          removed.insert(spillField, prev)
        }
      }
    }
  }

  /** Moves any fields in the top level not in `keep` to the struct `into`. Any
    * conflicting fields in `into` will have their names munged.
    *
    * Note that `keep` is an array of field names, not paths. This is because we
    * only care about what fields exist at the top level.
    */
  final case class MoveWildcard(
    field: ConcretePath,
    into: ConcretePath,
    keep: Set[String])
      extends Migration {
    def migrate(data: Data): Data = {
      val struct = data.get(field) match {
        case Some(s: MapV) => s
        case _ => throw new IllegalStateException(s"Expected struct at $field")
      }

      val intoStruct = data.get(into) match {
        case Some(s: MapV) => s
        case None          => MapV.empty
        case Some(_) =>
          throw new IllegalStateException(
            s"Refusing to overwrite existing into field at $into")
      }

      // These are all the fields that will be moved to the `into` path.
      val nonMatching = struct.elems
        .filter { case (name, _) => !keep.contains(name) }

      // Sort the fields by the number of leading `_` characters to produce
      // consistent behavior with name munging.
      //
      // Names that have the same number of leading `_` characters will never
      // conflict, so we can use an unstable sort here.
      val nonMatchingSorted = nonMatching.view.map(_._1).toArray
      Sorting.quickSort(nonMatchingSorted)(Ordering.by(_.indexWhere(_ != '_')))

      // Maps original names to munged names.
      val renameMap = MMap.empty[String, String]
      // A set of all names in `into`. Uses a `JHashSet` because `Set.add` doesn't
      // actually call `JHashSet.add`.
      val mungedNames = new JHashSet[String](intoStruct.elems.size)
      intoStruct.elems.foreach { case (name, _) => mungedNames.add(name) }

      nonMatchingSorted.foreach { name =>
        var munged = name
        var changed = false
        while (!mungedNames.add(munged)) {
          munged = s"_${munged}"
          changed = true
        }
        if (changed) {
          renameMap(name) = munged
        }
      }

      if (nonMatching.isEmpty) {
        // Don't add an empty struct to `into` path if there aren't any conflicts.
        data
      } else {
        // Remove all non-matching fields from the struct.
        val dataWithoutNonMatching = nonMatching.foldLeft(data) {
          case (acc, (name, _)) => acc.remove(field.append(name))
        }

        val dataWithoutNonMatching0 = data.get(into) match {
          case None          => dataWithoutNonMatching.insert(into, MapV.empty)
          case Some(MapV(_)) => dataWithoutNonMatching
          case Some(_) =>
            throw new IllegalStateException(
              s"Refusing to overwrite existing non-struct at $into")
        }

        // Add all non-matching fields to the `into` path.
        val munged = nonMatching.foldLeft(dataWithoutNonMatching0) {
          case (acc, (name, value)) =>
            val intoPath = into.append(renameMap.getOrElse(name, name))
            acc.insert(intoPath, value)
        }

        munged
      }
    }
  }
}
