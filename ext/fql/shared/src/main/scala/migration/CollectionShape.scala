package fql.migration

import fql.ast._
import fql.error.Error
import fql.typer.{ Type, Typer }
import fql.Result
import scala.collection.{ SeqMap => MaybeMutableSeqMap }
import scala.collection.immutable.SeqMap
import scala.collection.mutable.{ SeqMap => MSeqMap }

object CollectionShape {

  /** Builds a collection shape from the given collection.
    *
    * This is called over both the live and submitted schema, but it is always
    * passed a typer for the submitted schema. So, the `checked` flag is set to
    * `false` for the live schema, which results in unknown types being ignored.
    */
  def apply(
    schema: SchemaItem.Collection,
    typer: Typer,
    checked: Boolean = true): Result[CollectionShape] = {
    val errs = List.newBuilder[Error]

    val fields = schema.fields
      .flatMap {
        case f: Field.Defined =>
          def go(
            name: Name,
            te: SchemaTypeExpr,
            default: Option[Expr]): Option[(String, CollectionField)] = {

            te match {
              case SchemaTypeExpr.Object(teFields, wc, span) =>
                val fields = teFields.view.flatMap { (go _).tupled }.to(SeqMap)

                Some(
                  name.str -> CollectionField(
                    name,
                    FieldValue.Record(fields, wc.isDefined, span),
                    FieldSource.Schema(default)))

              case SchemaTypeExpr.Simple(te) =>
                if (checked) {
                  typer.typeTExprType(te) match {
                    case Result.Ok(ty) =>
                      Some(
                        name.str -> CollectionField(
                          name,
                          FieldValue(ty, FieldSource.Schema(default)),
                          source = FieldSource.Schema(default)))
                    case Result.Err(err) =>
                      errs ++= err
                      None
                  }
                } else {
                  // Types cannot have generics, so we can construct this
                  // TypeExpr.Scheme.
                  val ty =
                    typer.typeTSchemeUncheckedType(TypeExpr.Scheme(Seq.empty, te))
                  Some(
                    name.str -> CollectionField(
                      name,
                      FieldValue(ty, FieldSource.Schema(default)),
                      source = FieldSource.Schema(default)))
                }
            }
          }

          go(f.name, f.schemaType, f.value.map(_.value))

        case _ => None
      }
      .to(MaybeMutableSeqMap)

    val literalWildcard = schema.fields.collectFirst {
      case Field.Wildcard(te, span) =>
        typer.typeTExprType(te) match {
          case Result.Ok(ty) => CollectionWildcard(ty, WildcardSource.Schema, span)
          case Result.Err(err) =>
            errs ++= err
            CollectionWildcard(Type.Any, WildcardSource.Schema, span)
        }
    }

    val effectiveWildcard = if (literalWildcard.isEmpty && fields.isEmpty) {
      // No span for an implicit wildcard, but we get spans for explicit ones.
      Some(CollectionWildcard(Type.Any, WildcardSource.Implicit, Span.Null))
    } else {
      literalWildcard
    }

    val errors = errs.result()
    if (errors.isEmpty) {
      Result.Ok(CollectionShape(fields, effectiveWildcard))
    } else {
      Result.Err(errors)
    }
  }

  // This treats `Any` like `Top`, which makes schema constraints act like they
  // should.
  def isSchemaSubtype(typer: Typer, a: Type, b: Type): Boolean = (a, b) match {
    case (a, b) if a == b => true

    case (Type.Union(types, _), _) => types.forall(t => isSchemaSubtype(typer, t, b))
    case (_, Type.Union(types, _)) => types.exists(t => isSchemaSubtype(typer, a, t))

    case (Type.Intersect(types, _), _) =>
      types.exists(t => isSchemaSubtype(typer, t, b))
    case (_, Type.Intersect(types, _)) =>
      types.forall(t => isSchemaSubtype(typer, a, t))

    // `Any` is the supertype of everything.
    case (_, Type.Any(_)) => true
    // Nothing is a subtype of Any (this is the main difference between
    // `typer.isSubtype`).
    case (Type.Any(_), _) => false

    case _ => typer.isSubtype(a, b)
  }
}

/** Represents the shape of a collection. The fields are defined fields in schema,
  * and the wildcard is the effective wildcard. Specifically, if there are no fields
  * or wildcard defined in schema, then the wildcard here will be `Some(Any)`.
  */
case class CollectionShape(
  fields: MaybeMutableSeqMap[String, CollectionField],
  wildcard: Option[CollectionWildcard]) {
  def toMutable =
    MCollectionShape(fields.view.mapValues(_.toMutable).to(MSeqMap), wildcard)

  // Checks if `this` is compatible with `other`. This means that all of
  // `this.fields` are a supertype of `other.fields`.
  def compatibleWith(typer: Typer, prev: CollectionShape): Boolean = {
    (fields.keySet ++ prev.fields.keySet).forall { name =>
      (fields.get(name), prev.fields.get(name)) match {
        case (Some(field), Some(p)) =>
          field.source match {
            case FieldSource.Backfill(_) =>
              // Special case: a backfill source indicates we expected the field to
              // be added or involved in a split, but that didn't happen. It's an
              // unannounced guest.
              false
            case _ =>
              field.isPresent && p.isPresent && CollectionShape.isSchemaSubtype(
                typer,
                p.ty,
                field.ty)
          }

        case (Some(field), None) =>
          // If the field is being introduced, it must have come from an add
          // migration, a split migration, or a move.
          field.source match {
            case FieldSource.Add(_)          => true
            case FieldSource.Backfill(_)     => false
            case FieldSource.SplitInto(_, _) => true
            case FieldSource.Split(_)        => false
            case FieldSource.Drop(_)         => false
            case FieldSource.MovedInto(_)    => true

            // If all the nested fields of this field came from `add` migrations,
            // and there is no nested wildcard, then we allow this field.
            case FieldSource.Schema(_) =>
              def go(field: CollectionField): Boolean = {
                field.source match {
                  case FieldSource.Add(_) => true
                  case FieldSource.Schema(_) =>
                    field.value match {
                      case FieldValue.Record(fs, hasWild, _) =>
                        !hasWild && fs.values.forall { f => go(f) }

                      case _ => false
                    }
                  case _ => false
                }
              }

              wildcard.isEmpty && go(field)
          }

        // If the field `p` is being removed, it must have been removed by a
        // migration.
        case (None, Some(p)) => p.isRemoved

        // This is unreachable.
        case (None, None) => sys.error("unreachable")
      }
    } && ((wildcard, prev.wildcard) match {
      case (Some(w), Some(p)) => CollectionShape.isSchemaSubtype(typer, p.ty, w.ty)
      case (Some(w), None)    => w.isRemoved
      case (None, Some(p))    => p.isRemoved
      case (None, None)       => true // No wildcards on either is fine.
    })
  }

  def get(path: Path): Either[CollectionFieldError, CollectionField] = {
    var fields = this.fields

    // I'd like to `return` in this loop, so I'm using an iterator instead of a
    // `foreach`.
    val iter = path.elems.view.dropRight(1).iterator
    while (iter.hasNext) {
      iter.next() match {
        case PathElem.Field(name, _) =>
          fields.get(name) match {
            case Some(
                  CollectionField(
                    _,
                    FieldValue.Record(rec, wildcard, _),
                    FieldSource.Schema(_))) =>
              if (wildcard) {
                return Left(CollectionFieldError.WildcardNeighbor)
              }

              fields = rec

            case _ => return Left(CollectionFieldError.NotFound)
          }

        case _ => sys.error("Indexed fields are not supported")
      }
    }

    path.elems.last match {
      case PathElem.Field(name, _) =>
        fields.get(name).toRight(CollectionFieldError.NotFound)
      case _ => sys.error("Indexed fields are not supported")
    }
  }
}

sealed trait CollectionFieldError

object CollectionFieldError {
  final case object NotFound extends CollectionFieldError
  final case object WildcardNeighbor extends CollectionFieldError
}

object MCollectionShape {
  def empty = MCollectionShape(MSeqMap.empty, None)
}

// This constructor is private, because you should be using
// `CollectionShape.toMutable` instead.
case class MCollectionShape private (
  fields: MSeqMap[String, CollectionField],
  var wildcard: Option[CollectionWildcard]) {
  // Returns an immutable reference. Note that this is just a reference! This doesn't
  // copy anything.
  def asImmutable = CollectionShape(fields, wildcard)

  // Returns an immutable copy.
  def copyImmutable =
    CollectionShape(fields.view.mapValues(_.copyImmutable).to(SeqMap), wildcard)

  def put(path: Path, field: CollectionField): Unit = {
    var fields = this.fields

    val iter = path.elems.dropRight(1).iterator // I'd like to `return`
    while (iter.hasNext) {
      iter.next() match {
        case PathElem.Field(name, _) =>
          fields.get(name) match {
            case Some(CollectionField(_, FieldValue.Record(rec, _, _), _)) =>
              // This is a bit hacky. But, when constructing an `MCollectionShape`,
              // all the collection field values will be converted to `MSeqMap`. This
              // cast is mostly because parameterizing `CollectionField` or making an
              // `MCollectionField` class would be quite verbose.
              fields = rec.asInstanceOf[MSeqMap[String, CollectionField]]

            // All is well, we can ignore this `put`, as `Any` includes everything.
            case Some(CollectionField(_, FieldValue.Any, _)) =>
              return ()

            case _ =>
              throw new IllegalStateException(
                s"Expected a record as a parent of $path")
          }

        case _ => sys.error("Indexed fields are not supported")
      }
    }

    path.elems.last match {
      case PathElem.Field(name, _) => fields.put(name, field)
      case _                       => sys.error("Indexed fields are not supported")
    }

    ()
  }
}

case class CollectionField(name: Name, value: FieldValue, source: FieldSource) {
  def toMutable: CollectionField = copy(value = value.toMutable)
  def copyImmutable: CollectionField = copy(value = value.copyImmutable)

  def isRemoved = !isPresent

  def ty = value.ty

  def isPresent = source match {
    case FieldSource.Schema(_)       => true
    case FieldSource.Split(_)        => true
    case FieldSource.SplitInto(_, _) => false
    case FieldSource.Drop(_)         => true
    case FieldSource.Backfill(_)     => true
    case FieldSource.Add(_)          => false
    case FieldSource.MovedInto(_)    => false
  }

  def isRequired(typer: Typer): Boolean = !isOptional(typer)

  def isOptional(typer: Typer): Boolean = {
    // Special case drop: drop is never optional, even though the `Any` is a subtype
    // of null.
    source match {
      case FieldSource.Drop(_) => false
      case _ =>
        def hasDefault = source match {
          case FieldSource.Schema(Some(_)) => true
          case _                           => false
        }

        isRemoved || hasDefault || typer.isSubtype(Type.Null, ty)
    }
  }
}

sealed trait FieldValue {
  def toMutable: FieldValue = this match {
    case FieldValue.Literal(ty) => FieldValue.Literal(ty)
    case FieldValue.Record(fs, wc, span) =>
      FieldValue.Record(fs.view.mapValues(_.toMutable).to(MSeqMap), wc, span)
  }
  def copyImmutable: FieldValue = this match {
    case FieldValue.Literal(ty) => FieldValue.Literal(ty)
    case FieldValue.Record(fs, wc, span) =>
      FieldValue.Record(fs.view.mapValues(_.copyImmutable).to(SeqMap), wc, span)
  }

  def ty: Type
}

object FieldValue {
  def apply(ty: Type, source: FieldSource): FieldValue = ty match {
    case Type.Record(fs, wc, span) =>
      Record(
        fs.map { case (k, v) =>
          k -> CollectionField(Name(k, Span.Null), FieldValue(v, source), source)
        },
        wc.isDefined,
        span)
    case _ => Literal(ty)
  }

  val Any = Literal(Type.Any)
  def Union(tys: Seq[Type], span: Span) = Literal(Type.Union(tys, span))

  case class Literal(ty: Type) extends FieldValue {
    ty match {
      case Type.Record(_, _, _) =>
        sys.error("Records must be constructed with `FieldValue.Record`")
      case _ => ()
    }
  }
  case class Record(
    fields: MaybeMutableSeqMap[String, CollectionField],
    wildcard: Boolean,
    span: Span)
      extends FieldValue {
    def ty = Type.Record(
      fields.view.filter(_._2.isPresent).mapValues(_.ty).to(SeqMap),
      None,
      span)
  }
}

sealed trait FieldSource

object FieldSource {
  // This field was defined in schema.
  case class Schema(default: Option[Expr]) extends FieldSource

  // This field was the result of a `split` migration.
  case class Split(value: MigrationItem.Split) extends FieldSource

  // This field was merged into another from a `split` migration.
  case class SplitInto(value: MigrationItem.Split, target: Path) extends FieldSource

  // This field was the result of a `drop` migration (it'll always have type `Any` in
  // this case).
  case class Drop(value: MigrationItem.Drop) extends FieldSource

  // This field was the result of a `backfill` migration. It doesn't really exist,
  // but we keep it around so that split migrations can consume it.
  case class Backfill(value: MigrationItem.Backfill) extends FieldSource

  // The field was added.
  case class Add(value: MigrationItem.Add) extends FieldSource

  // The field was moved into.
  case class MovedInto(value: MigrationItem.Move) extends FieldSource
}

case class CollectionWildcard(ty: Type, source: WildcardSource, span: Span) {
  def isRemoved = !isPresent

  def isPresent = source match {
    case WildcardSource.Implicit       => true
    case WildcardSource.Schema         => true
    case WildcardSource.MoveWildcard   => true
    case WildcardSource.AddWildcard(_) => false
  }
}

sealed trait WildcardSource

object WildcardSource {
  // This wildcard was added implicitly, because there were no fields in the schema.
  object Implicit extends WildcardSource
  // This wildcard was defined in the submitted schema.
  object Schema extends WildcardSource
  // This wildcard was added by `move_wildcard`.
  object MoveWildcard extends WildcardSource
  // This wildcard was removed by rewinding `add_wildcard`.
  case class AddWildcard(span: Span) extends WildcardSource
}
