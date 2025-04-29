package fauna.model.schema.index

import fauna.model.schema.FieldPath
import fauna.repo.schema.migration.Migration
import fauna.repo.schema.CollectionSchema
import fauna.storage.doc.ConcretePath
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.{ Map => MMap, Set => MSet }

class IndexFieldProvenance {
  import IndexFieldProvenance._

  var hasMigrated = false

  // A map of field changes. The key is the current field name, and the value is
  // where that field came from.
  //
  // As `migrate` is called, the keys will be updated as fields are moved around, and
  // their original name will be tracked on `Field` instances.
  //
  // Once all migrations have been applied, this will be a map of updated names to
  // where those names originated.
  val fields = MMap.empty[String, Field]

  // `move_wildcard` moves all fields _except_ a list of fields. Any time that
  // migration is used, those fields that have not been used are added to this set.
  var wildcardModified = false
  val unmodifiedFields = MSet.empty[String]

  def migrate(migration: Migration) = {
    hasMigrated = true
    migration match {
      // If there is not a wildcard in schema, adding a field doesn't really modify
      // anything. However, indexes shouldn't be able to point to fields outside of
      // schema when there is no wildcard, so this is fine as-is.
      case Migration.AddField(path, _, _) => modify(path)
      case Migration.DropField(path)      => modify(path)
      case Migration.MoveField(from, to)  => rename(from, to)
      case Migration.SplitField(field, spill, _, _, _) =>
        modify(field)
        modify(spill)

      // This moves any conflicts from past `AddField` migrations into `into`. Those
      // fields have already been tracked as modified, so we just modify `into`.
      case Migration.MoveConflictingFields(into) =>
        modify(into)

      case Migration.MoveWildcard(_, into, except) =>
        modify(into)
        // All fields except `except` get modified in this migration.
        wildcardModified = true
        unmodifiedFields ++= except
    }
  }

  private def modify(path: ConcretePath) = {
    insert(path, Field.Modified)
  }

  private def rename(from: ConcretePath, to: ConcretePath): Unit = {
    val newField = get(from) match {
      case Some(Field.Renamed(original))          => Field.Renamed(original)
      case Some(Field.Modified | Field.Record(_)) => Field.Modified
      case None                                   => Field.Renamed(from)
    }

    insert(to, newField)
  }

  private def get(path: ConcretePath): Option[Field] = {
    var curr = fields

    val iter = path.path.iterator // I want `return`
    while (iter.hasNext) {
      val elem = iter.next()
      curr.get(elem) match {
        case Some(Field.Record(children)) => curr = children
        case Some(f)                      => return Some(f)
        case _                            => return None
      }
    }

    if (curr.isEmpty) {
      None
    } else {
      Some(Field.Modified)
    }
  }

  private def insert(path: ConcretePath, value: Field): Unit = {
    var curr = fields

    val iter = path.path.slice(0, path.path.size - 1).iterator // I want `return`
    while (iter.hasNext) {
      val elem = iter.next()
      curr.get(elem) match {
        case Some(Field.Record(children)) => curr = children

        // This `insert` was to modify the child of a field that has already been
        // modified or renamed. So, mark it modified, and stop iterating.
        case Some(_) =>
          curr.put(elem, Field.Modified)
          return

        case None =>
          val children = MMap.empty[String, Field]
          curr.put(elem, Field.Record(children))
          curr = children
      }
    }

    // If we've gotten this far, then `curr` is the parent element of the last
    // element of `path`.
    curr.put(path.path.last, value)
  }

  // Fields that aren't tracked may or may not fall into the wildcard migration. For
  // untracked fields, this checks if those fields have been migrated or not.
  private def wildcardHasModified(path: ConcretePath): Boolean = {
    // FIXME: Dedupe with ReadBroker and CollectionSchema.
    val modifiedField = path.path match {
      case ArraySeq("data", field, _*) => field
      case ArraySeq(field, _*) if CollectionSchema.MetaFields.contains(field) =>
        field
      case _ => throw new IllegalStateException(s"Unsupported path: $path")
    }

    if (wildcardModified) {
      if (unmodifiedFields.contains(modifiedField)) {
        // The wildcard has been modified, but `name` is one of the defined fields
        // left alone.
        false
      } else {
        // The wildcard has been modified, and `name` was modified. Indexes covering
        // this field need to be rebuilt.
        true
      }
    } else {
      // Simple case: the wildcard hasn't been modified.
      false
    }
  }

  /** Maps an updated index shape to an old index shape.
    */
  def mapShape(updated: IndexShape): Option[IndexShape] = {
    var modified = false

    val oldTerms = updated.terms.map { t =>
      mapField(t.field) match {
        case Some(f) => t.copy(field = f)
        case None =>
          modified = true
          t
      }
    }
    val oldValues = updated.values.map { t =>
      mapField(t.field) match {
        case Some(f) => t.copy(field = f)
        case None =>
          modified = true
          t
      }
    }

    if (modified) {
      None
    } else {
      Some(IndexShape(oldTerms, oldValues))
    }
  }

  private def mapField(field: FieldShape): Option[FieldShape] = {
    field match {
      case FieldShape.Fixed(path) =>
        val oldPath = mapPath(path) match {
          case Some(oldPath) => oldPath
          case None          => return None
        }
        Some(FieldShape.Fixed(oldPath))

      // If any migrations are applied, all computed fields must be rebuilt.
      case c: FieldShape.Computed =>
        if (hasMigrated) {
          None
        } else {
          Some(c)
        }
    }

  }

  /** Maps an updated field path to an old field path. Returns `None` if the field has been migrated.
    */
  private def mapPath(field: FieldPath): Option[FieldPath] = {
    if (field.exists(_.isLeft)) {
      // Rebuild if there are any migrations, and leave as-is if there are no
      // migrations.
      //
      // Non-strings can't be migrated, so just give up. Theoretically, we could do a
      // little better here (for example, if indexing into an element of an array
      // that got renamed), but that's such an edge case that we can just rebuild.
      if (fields.nonEmpty || wildcardModified) {
        return None
      } else {
        return Some(field)
      }
    }

    val stringPath = field
      .map {
        case Right(s) => s
        case Left(_)  => sys.error("unreachable")
      }
      .to(ArraySeq)

    val dataPath = stringPath match {
      case ArraySeq("data") =>
        if (fields.nonEmpty || wildcardModified) {
          return None
        } else {
          return Some(field)
        }

      case ArraySeq(f, _*) if CollectionSchema.MetaFields.contains(f) => stringPath
      case _ => "data" +: stringPath
    }
    val path = ConcretePath(dataPath.to(ArraySeq))

    val originalPath = get(path) match {
      case Some(Field.Renamed(original))          => original
      case Some(Field.Modified | Field.Record(_)) => return None
      case None                                   => path
    }

    if (wildcardHasModified(originalPath)) {
      None
    } else {
      // This matches against the user's new path, and keeps the `data` prefix if
      // they specified it. This means updating an index from `.data.name` to `.name`
      // will rebuild it, but otherwise it handles migrations nicely.
      //
      // FIXME: Clean this up so we use `ConcretePath` throughout the whole
      // `CollectionIndexManager`, instead of doing this hacky conversion.
      val userPath = stringPath.head match {
        case f if CollectionSchema.MetaFields.contains(f) => originalPath.path
        case _                                            => originalPath.path.tail
      }

      Some(userPath.map { s => Right(s) }.toList)
    }
  }
}

object IndexFieldProvenance {
  case class IndexShape(terms: Vector[TermShape], values: Vector[ValueShape])

  case class TermShape(field: FieldShape, mva: Boolean)
  case class ValueShape(field: FieldShape, ascending: Boolean, mva: Boolean)

  sealed trait FieldShape {
    def path: FieldPath
  }

  object FieldShape {
    case class Fixed(path: FieldPath) extends FieldShape
    case class Computed(path: FieldPath, body: String) extends FieldShape

    def apply(field: CollectionIndex.Field): FieldShape = {
      field match {
        case f: CollectionIndex.Field.Fixed =>
          FieldShape.Fixed(f.fieldPath)
        case f: CollectionIndex.Field.Computed =>
          FieldShape.Computed(f.fieldPath, f.body)
      }
    }
  }

  sealed trait Field

  def allShapes(mgr: CollectionIndexManager)
    : Map[IndexShape, Seq[CollectionIndexManager.IndexOrUnique]] = {
    val res = MMap.empty[IndexShape, Seq[CollectionIndexManager.IndexOrUnique]]

    mgr.userDefinedIndexes.values.foreach { index =>
      val shape = IndexShape(index)
      res.get(shape) match {
        case Some(indexes) => res.update(shape, indexes :+ Left(index))
        case None          => res += shape -> Seq(Left(index))
      }
    }
    mgr.uniqueConstraints.foreach { unique =>
      val shape = IndexShape(unique.toIndexDefinition)
      res.get(shape) match {
        case Some(indexes) => res.update(shape, indexes :+ Right(unique))
        case None          => res += shape -> Seq(Right(unique))
      }
    }

    res.toMap
  }

  object IndexShape {
    def apply(index: CollectionIndexManager.IndexDefinition): IndexShape = {
      IndexShape(
        terms = index.terms.map(TermShape(_)),
        values = index.values.map(ValueShape(_))
      )
    }
  }

  object TermShape {
    def apply(term: CollectionIndex.Term): TermShape = {
      TermShape(
        field = FieldShape(term.field),
        mva = term.isMVA
      )
    }
  }

  object ValueShape {
    def apply(value: CollectionIndex.Value): ValueShape = {
      ValueShape(
        field = FieldShape(value.field),
        ascending = value.ascending,
        mva = value.isMVA
      )
    }
  }

  object Field {
    // The field was renamed from the absolute path `from`.
    case class Renamed(from: ConcretePath) extends Field

    // The field was modified by a migration.
    case object Modified extends Field

    // This is a record, where some children fields are modified. The record itself
    // is always considered modified, as some of its children are always renamed or
    // modified.
    case class Record(children: MMap[String, Field]) extends Field
  }
}
