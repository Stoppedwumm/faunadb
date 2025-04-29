package fauna.model.schema.index

import fauna.atoms.{ IndexID, ScopeID }
import fauna.model.schema.FieldPath
import fauna.model.schema.NativeIndex
import fauna.repo.values.{ Value => RValue, ValueDecoder }
import fauna.repo.IndexConfig
import fql.ast.Config
import fql.ast.Span

object CollectionIndex {
  sealed trait Status {
    def asStr: String
  }

  object Status {
    def fromStr(toString: String): Option[Status] = {
      toString match {
        case "building" => Some(Status.Building)
        case "complete" => Some(Status.Complete)
        case "failed"   => Some(Status.Failed)
        case _          => None
      }
    }

    object Building extends Status { def asStr = "building" }
    object Complete extends Status { def asStr = "complete" }
    object Failed extends Status { def asStr = "failed" }
  }

  sealed trait Field {

    def path: String

    // The path but always starting with '.'.
    def dotPath: String = if (path.startsWith(".")) path else s".$path"

    def parsePath: fql.ast.Path = FieldPath
      .parse(path)
      .getOrElse(throw new IllegalStateException(s"invalid path: $path"))

    def fieldPath: FieldPath = parsePath.toList

    // The the name of the component that heads the fieldpath, if it exists.
    // If the entry is computed, this will name a computed field.
    def head: Option[String] = fieldPath match {
      case Right(name) :: _ => Some(name)
      case _                => None
    }

    def matches(other: Field): Boolean = (this, other) match {
      case (Field.Fixed(_), Field.Fixed(_)) => fieldPath == other.fieldPath
      case (Field.Computed(_, body), Field.Computed(_, otherBody)) =>
        fieldPath == other.fieldPath && body == otherBody
      case _ => false
    }

    def isComputed: Boolean = this match {
      case _: Field.Fixed    => false
      case _: Field.Computed => true
    }
  }

  object Field {
    case class Fixed(path: String) extends Field

    // NB: `path` is a field path whose first component is the computed field name.
    case class Computed(path: String, body: String) extends Field {

      // Returns the name of the computed field from the given path.
      def fieldName: String = path match {
        case FieldPath(Right(name) :: _) => name
        case FieldPath(_) =>
          throw new IllegalStateException(
            s"computed field path started with a number: $path")
        case _ =>
          throw new IllegalStateException(s"computed field path didn't parse: $path")
      }
    }
  }

  trait Entry {
    def field: Field
    def fieldPath: FieldPath = field.fieldPath
    def mvaOpt: Option[Boolean]
    def isMVA: Boolean = mvaOpt.getOrElse(false)

    def isIDField = field.path == ".id" || field.path == "id"
  }

  case class Term(field: Field, mvaOpt: Option[Boolean]) extends Entry {

    def matches(other: Term): Boolean =
      field.matches(other.field) && isMVA == other.isMVA

    def toConfig =
      Config.IndexTerm(path = field.parsePath, mva = isMVA, span = Span.Null)

    // NB: Terms encode _without_ the body of the field if it is computed.
    //     Why? Because the body shouldn't be visible in the term, but
    //     our internal field mechanism only works at the top-level.
    def toValue: RValue.Struct = {
      val b = RValue.Struct.newBuilder
      b.addOne("field" -> RValue.Str(field.path))
      mvaOpt foreach { mva => b.addOne("mva" -> RValue.Boolean(mva)) }
      b.result()
    }
  }

  object Term {
    // IMPORTANT: As the computed field body isn't encoded with a term, it
    // always decodes with a fixed field. Decoded terms must be
    // cross-referenced with the collection schema to get the correct
    // field type. See CollectionIndexManager.adjustTerms.
    implicit object IndexTermDecoder extends ValueDecoder[Term] {
      def apply(value: RValue): Term = {
        val path = (value / "field").as[String]
        val mva = (value / "mva").asOpt[Boolean]
        Term(Field.Fixed(path), mva)
      }
    }
  }

  case class Value(field: Field, ascending: Boolean, mvaOpt: Option[Boolean])
      extends Entry {

    def matches(other: Value): Boolean = field.matches(
      other.field) && ascending == other.ascending && isMVA == other.isMVA

    def toConfig =
      Config.IndexValue(
        path = field.parsePath,
        mva = isMVA,
        asc = ascending,
        span = Span.Null)

    // NB: Values encode _without_ the body of the field if it is computed.
    //     Why? Because the body shouldn't be visible in the value, but
    //     our internal field mechanism only works at the top-level.
    def toValue: RValue.Struct = {
      val b = RValue.Struct.newBuilder
      b.addOne("field" -> RValue.Str(field.path))
      b.addOne("order" -> RValue.Str(if (ascending) "asc" else "desc"))
      mvaOpt foreach { mva => b.addOne("mva" -> RValue.Boolean(mva)) }
      b.result()
    }
  }

  object Value {
    // IMPORTANT: As the computed field body isn't encoded with a value, it
    // always decodes with a fixed field. Decoded values must be
    // cross-referenced with the collection schema to get the correct
    // field type. See CollectionIndexManager.adjustValues.
    implicit object IndexValueDecoder extends ValueDecoder[Value] {
      def apply(value: RValue): Value = {
        val path = (value / "field").as[String]
        val order = (value / "order").asOpt[String] getOrElse "asc"
        val mva = (value / "mva").asOpt[Boolean]
        val asc = if (order == "asc") true else false
        Value(Field.Fixed(path), asc, mva)
      }
    }
  }
}

sealed trait CollectionIndex {
  def indexID: IndexID
  def terms: Vector[CollectionIndex.Term]
  def values: Vector[CollectionIndex.Value]
  def status: CollectionIndex.Status
}

final case class UserDefinedIndex(
  indexID: IndexID,
  terms: Vector[CollectionIndex.Term],
  values: Vector[CollectionIndex.Value],
  queryable: Boolean,
  status: CollectionIndex.Status,
  name: String)
    extends CollectionIndex

final case class UniqueConstraintIndex(
  indexID: IndexID,
  terms: Vector[CollectionIndex.Term],
  values: Vector[CollectionIndex.Value],
  status: CollectionIndex.Status
) extends CollectionIndex

final case class NativeCollectionIndex(
  config: IndexConfig.Native,
  terms: Vector[CollectionIndex.Term],
  values: Vector[CollectionIndex.Value]
) extends CollectionIndex {
  def indexID = config.id
  def status = CollectionIndex.Status.Complete
}

object NativeCollectionIndex {
  def Documents(scope: ScopeID) =
    NativeCollectionIndex(
      NativeIndex.DocumentsByCollection(scope),
      Vector(
        CollectionIndex
          .Term(CollectionIndex.Field.Fixed(".coll"), Some(false))),
      Vector.empty)

  def Changes(scope: ScopeID) =
    NativeCollectionIndex(
      NativeIndex.ChangesByCollection(scope),
      Vector(
        CollectionIndex
          .Term(CollectionIndex.Field.Fixed(".coll"), mvaOpt = Some(false))),
      Vector(
        CollectionIndex.Value(
          CollectionIndex.Field.Fixed(".ts"),
          ascending = true,
          mvaOpt = Some(false)))
    )
}
