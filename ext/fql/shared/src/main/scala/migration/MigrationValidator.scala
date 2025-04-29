package fql.migration

import fql.ast._
import fql.ast.display._
import fql.error.{ Error, Hint }
import fql.migration.MigrationValidator.suggestMigration
import fql.typer.{ Constraint, Type, Typer }
import fql.Result
import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

final case class MigrationError(
  message: String,
  span: Span,
  override val hints: Seq[Hint] = Nil)
    extends Error

object MigrationValidator {

  /** A Path is a starting shape and sequence of migrations to bring the shape
    * back to the target shape it was rewound from.
    */
  final case class Path(
    shape: CollectionShape,
    migrations: Seq[SchemaMigration],
    span: Span)

  // Rudimentary helper for creating hints that suggest adding a migration. It
  // deals with the migration block possibly being empty.
  // Caveat emptor.
  def suggestMigration(
    block: Option[Member.Typed[MigrationBlock]],
    noBlockSpan: Span, // Where to put the suggestion if the block is empty.
    msg: String,
    ms: Seq[String] // The migrations to suggest.
  ) =
    block match {
      case Some(block) =>
        Hint(
          msg,
          // -1 gives us the character before the closing `}`.
          block.span.copy(start = block.span.end - 1, end = block.span.end - 1),
          Option.when(ms.nonEmpty) { ms.mkString("  ", "\n    ", "\n") }
        )
      case None =>
        Hint(
          msg,
          noBlockSpan,
          Option.when(ms.nonEmpty) {
            s"""|migrations {
                |  ${ms.mkString("  ", "\n    ", "")}
                |  }
                |""".stripMargin
          }
        )
    }

  /** Given live and submitted schemas, derive the sequence of internal migrations
    * that should be applied to the live schema to obtain the submitted schema.
    */
  def validate(
    typer: Typer,
    live: SchemaItem.Collection,
    submitted: SchemaItem.Collection): Result[Seq[SchemaMigration]] =
    for {
      liveShape      <- CollectionShape(live, typer, checked = false)
      submittedShape <- CollectionShape(submitted, typer)
      mp <- new MigrationValidator(typer, submitted, submittedShape)
        .rewindToContinuation(live)
      migrations <- okOrExplain(typer, mp, liveShape, submittedShape, submitted)
    } yield migrations

  /** Given a submitted schema, validate that all migrations make sense and can be
    * applied to some schema in order to obtain the submitted schema. Return the
    * derived sequence of migrations.
    * Useful for validation prior to submission to the database, via FQLAnalyzer.
    */
  def validate(
    typer: Typer,
    submitted: SchemaItem.Collection): Result[Seq[SchemaMigration]] =
    for {
      submittedShape <- CollectionShape(submitted, typer)
      p <- new MigrationValidator(typer, submitted, submittedShape).rewindAll
    } yield p.migrations

  // Given a path of migrations, check that the path's origin is compatible with the
  // live schema. If the origin and live schema are not compatible, attempt to
  // explain why not with errors.
  private def okOrExplain(
    typer: Typer,
    path: Path,
    liveShape: CollectionShape,
    submittedShape: CollectionShape,
    submitted: SchemaItem.Collection): Result[Seq[SchemaMigration]] =
    if (path.shape.compatibleWith(typer, liveShape)) {
      Result.Ok((path.migrations).toSeq)
    } else {
      // Copy over fields from the origin to the submitted shape because rewinding
      // populates some fields with information useful in explaining incompatibility.
      val shape = submittedShape.toMutable
      path.shape.fields.values.foreach { field =>
        shape.fields(field.name.str) = field
      }
      Result.Err(
        new IncompatibilityExplainer(typer, shape.asImmutable, submitted)
          .explain(liveShape)
          .toSeq)
    }
}

/** One-shot class that finds a migration path connecting a live schema to a
  * to a submitted one.
  */
private class MigrationValidator(
  val typer: Typer,
  val target: SchemaItem.Collection,
  val targetShape: CollectionShape) {
  import MigrationValidator._

  private val errsB = new ListBuffer[Error]
  private val shape = targetShape.toMutable
  private val migrations: ListBuffer[SchemaMigration] = ListBuffer.empty
  private def addMigration(m: SchemaMigration): Unit =
    // Rewinding generates migrations in reverse order.
    migrations.prepend(m)

  private var currentMoveConflicts: Option[fql.ast.Path] = None

  // Rewind rewinds the submitted schema back to the migration continuation point,
  // returning a path that should link the live schema to the submitted schema.
  def rewindToContinuation(live: SchemaItem.Collection) = rewind(
    migrationsToApply(live))

  // Rewind all rewinds the submitted schema through all its migrations.
  def rewindAll = rewind(target.migrations.fold(Seq.empty[MigrationItem]) {
    _.config.items
  })

  private def rewind(ms: Seq[MigrationItem]) = {
    ms.reverse foreach rewindOne
    val rewoundShape = shape.copyImmutable
    validateShape(ms, rewoundShape)
    result(
      Path(
        rewoundShape,
        migrations.toList,
        target.span.copy(start = target.span.end - 1)))
  }

  // migrationsToApply finds the continuation point for migrations, which is just
  // after the longest prefix of the submitted schema's migrations that matches with
  // a suffix of the live schema's migrations. It returns the suffix of the submitted
  // schema's migrations that must take the live schema to a schema compatible with
  // the submitted schema.
  private def migrationsToApply(live: SchemaItem.Collection): Seq[MigrationItem] = {
    val tms = target.migrations.fold(Seq.empty[MigrationItem]) {
      _.config.items
    }
    val tmsNoSpan = tms.map { _.withNullSpan }
    val lmsNoSpan = live.migrations.fold(Seq.empty[MigrationItem]) {
      _.config.items.map { _.withNullSpan }
    }

    // A bit C-ish but it works.
    var i = 0
    var done = false
    while (!done && i <= tms.size) {
      if (lmsNoSpan.endsWith(tmsNoSpan.dropRight(i))) {
        done = true
      } else {
        i += 1
      }
    }
    tms.drop(tms.size - i)
  }

  // Rewind a backfill migration. Backfills complete migrations that add fields,
  // like add and split, when the new fields aren't nullable and don't have defaults,
  // although a backfill can still be specified in those cases. Rewinding a backfill
  // adds a marker to the shape that an add or split migration must consume.
  def rewindBackfill(bf: MigrationItem.Backfill): Unit = bf match {
    case MigrationItem.Backfill(field, expr, span) =>
      shape.asImmutable.get(field) match {
        case Right(fld) =>
          // FIXME: Plumb deadline through MigrationValidator.
          implicit val deadline = Int.MaxValue.seconds.fromNow
          val sigtc = typer.typeAnnotatedExpr(bf.value.expr, fld.ty)

          sigtc.failures match {
            case Seq(head, tail @ _*) =>
              errsB += head.copy(_hints =
                head._hints :+ Hint("Field type defined here", fld.ty.span))
              errsB ++= tail
            case _ =>
          }

          fld.source match {
            case FieldSource.Backfill(b) =>
              errsB += MigrationError(
                s"Cannot backfill field `${field.display}` multiple times",
                b.field.span,
                Seq(
                  Hint("Earlier backfill defined here", field.span),
                  Hint("Delete the later backfill", b.span, Some(""))
                )
              )

            case FieldSource.SplitInto(laterSplit, laterTarget) =>
              // NB: The rewind will also error on the add, if it's present.
              errsB += MigrationError(
                s"Cannot split field `${field.display}`: cannot split into a field after it's backfilled",
                field.span,
                Seq(
                  Hint("Field is an output of a split", laterTarget.span),
                  Hint(
                    "Move the backfill after the split",
                    laterSplit.span.to(laterSplit.span),
                    Some(s"backfill ${field.display} = ${expr.expr.display}")),
                  Hint("Remove the backfill", span, Some("")),
                  Hint("Remove the split", laterSplit.span, Some(""))
                )
              )

            case FieldSource.Add(add) =>
              errsB += MigrationError(
                s"Cannot add field `${field.display}` after it's backfilled",
                add.span,
                Seq(
                  Hint("Field backfilled here", span)
                  // TODO: Hard to suggest something in this case.
                )
              )

            case _ if fld.isRemoved =>
              errsB += MigrationError(
                s"Cannot backfill field `${field.display}, as it is not in the submitted schema",
                field.span
              )

            case _ =>
              // An earlier add or split migration is responsible for consuming this.
              // The source indicates this field needs to be declared by some earlier
              // migration, which will update the shape.
              shape.put(field, fld.copy(source = FieldSource.Backfill(bf)))
          }

        case Left(CollectionFieldError.NotFound) =>
          errsB += MigrationError(
            s"Cannot backfill field `${field.display}`, as it is not in the submitted schema",
            field.span)
        case Left(CollectionFieldError.WildcardNeighbor) =>
          errsB += MigrationError(
            s"Cannot backfill field, as there is a wildcard in the same struct as `${field.display}`",
            field.span)
      }
  }

  // Rewind a drop migration.
  def rewindDrop(d: MigrationItem.Drop): Unit = d match {
    case MigrationItem.Drop(field, _) =>
      addMigration(SchemaMigration.Drop(field))

      shape.asImmutable.get(field) match {
        case Right(shape) if shape.isPresent =>
          errsB += MigrationError(
            s"Cannot drop field `${field.display}`. Dropped fields must be removed from schema",
            field.span,
            Seq(Hint("Field defined here", shape.name.span)))

        case Right(_) | Left(CollectionFieldError.NotFound) | Left(
              CollectionFieldError.WildcardNeighbor) =>
          val name = field.elems.last match {
            case PathElem.Field(name, span) => Name(name, span)
            case _ =>
              throw new IllegalStateException(
                "Last element in a migration path must be a field")
          }

          field.parent match {
            case Some(parent) =>
              shape.asImmutable.get(parent) match {
                case Right(CollectionField(_, FieldValue.Record(_, _, _), _)) =>
                  shape.put(
                    field,
                    CollectionField(
                      name,
                      FieldValue.Any,
                      source = FieldSource.Drop(d)))

                case Right(CollectionField(_, _, _)) =>
                  errsB += MigrationError(
                    s"Cannot drop field `${field.display}`, as the parent is not an object",
                    field.span)

                case Left(_) =>
                  errsB += MigrationError(
                    s"Cannot drop field `${field.display}`, as the parent object was removed",
                    field.span)
              }

            case None =>
              shape.put(
                field,
                CollectionField(name, FieldValue.Any, source = FieldSource.Drop(d)))
          }
      }
  }

  // Rewind a move. Note that while a move does add a new field to the schema,
  // it's not considered to be declaring a new field like add or split. The
  // upshot is that a move does not consume a backfill; however, it will
  // propagate a backfill to the un-moved field.
  // TODO: Should we allow that? It's logical but why would you do it?
  def rewindMove(r: MigrationItem.Move): Unit = r match {
    case MigrationItem.Move(field, to, span) =>
      // `to` must exist, and `field` must not exist.
      shape.asImmutable.get(to) match {
        case Right(_) if field == to =>
          // Forbid pointless `move x x` migrations.
          errsB += MigrationError(
            s"Cannot move a field to itself",
            field.span,
            Seq(Hint("Remove the futile `move`", span, Some("")))
          )
        case Right(t) =>
          // Even if we emit this error, we still edit the shape to produce
          // fewer errors.
          shape.asImmutable.get(field) match {
            case Right(f) if f.isPresent && f.isRequired(typer) =>
              errsB += MigrationError(
                s"Cannot move field `${field.display}`, as it is in the submitted schema",
                field.span,
                Seq(
                  Hint("Field defined here", f.name.span),
                  Hint(
                    s"To move `${field.display}` to `${to.display}` and add a new field named `${f.name.str}`, add an `add` migration",
                    to.span.copy(start = to.span.end),
                    Some(s"\n    add ${field.display}")
                  )
                )
              )

            case Right(f) if f.isPresent =>
              f.source match {
                case FieldSource.Schema(Some(default)) =>
                  addMigration(SchemaMigration.Add(field, f.ty, Some(default)))
                case _ => ()
              }

            case Right(_) | Left(CollectionFieldError.NotFound) => ()

            case Left(CollectionFieldError.WildcardNeighbor) =>
              errsB += MigrationError(
                s"Cannot move field `${field.display}` out of a struct with a wildcard definition",
                field.span)
          }

          field.parent.map(shape.asImmutable.get(_)) match {
            case Some(Left(_)) =>
              errsB += MigrationError(
                s"Cannot move field `${field.display}`, as the parent object was removed.",
                field.span)

              shape.put(to, t.copy(source = FieldSource.MovedInto(r)))
            case Some(Right(CollectionField(_, FieldValue.Literal(_), source)))
                if !source.isInstanceOf[FieldSource.Drop] =>
              errsB += MigrationError(
                s"Cannot move field `${field.display}`, as the parent value is not an object.",
                field.span)

              shape.put(to, t.copy(source = FieldSource.MovedInto(r)))

            case None | Some(Right(CollectionField(_, _, _))) =>
              addMigration(SchemaMigration.Move(field, to))

              val name = field.elems.last match {
                case PathElem.Field(name, span) => Name(name, span)
                case _ =>
                  throw new IllegalStateException(
                    "Last element in a migration path must be a field")
              }

              shape.put(field, t.copy(name = name))
              shape.put(to, t.copy(source = FieldSource.MovedInto(r)))
          }

        case Left(CollectionFieldError.NotFound) =>
          errsB += MigrationError(
            s"Cannot move field `${field.display}`, as it is not in the live schema",
            field.span)

        case Left(CollectionFieldError.WildcardNeighbor) =>
          errsB += MigrationError(
            s"Cannot move field `${to.display}` into a struct with a wildcard definition",
            to.span)
      }
  }

  // Rewind a split migration. A split is considered to add the fields that are split
  // into, so those fields do not need to be added, but may need backfills after they
  // are declared by the split. Therefore, split consumes backfills. Split also
  // consumes a backfill for the split source, because the source may not be nullable
  // or have a default and yet end up empty as a result of the split.
  def rewindSplit(spl: MigrationItem.Split): Unit = spl match {
    case MigrationItem.Split(source, targets, span) =>
      if (targets.size < 2) {
        // Parsing and validation should forbid this.
        // TODO: Would be nice to statically prevent this.
        errsB += MigrationError(
          "Cannot split field: expected at least two fields to split into",
          span
        )
        return
      }

      // Because split consumes the source field, it cannot already be present unless
      // it is also a target.
      shape.asImmutable.get(source) match {
        case Right(f) if f.isPresent && !targets.contains(source) =>
          f.source match {
            case FieldSource.Schema(_) | FieldSource.Backfill(_) =>
              // TODO: Improve span to point to the entire declaration, so we
              //            can suggest removing the declaration.
              errsB += MigrationError(
                "Cannot split field: field is still present in submitted schema",
                source.span,
                Seq(Hint("Field defined here", f.name.span))
              )
            case FieldSource.Split(laterSplit) =>
              errsB += MigrationError(
                "Cannot split field twice",
                laterSplit.from.span,
                Seq(
                  Hint("Field is also split here", spl.from.span),
                  Hint("Remove this split", span, Some("")),
                  Hint("Remove the later split", laterSplit.span, Some(""))
                )
              )
            case FieldSource.Drop(drop) =>
              errsB += MigrationError(
                "Cannot drop field that was split",
                source.span,
                Seq(Hint("Remove the `drop`", drop.span, Some("")))
              )
            case FieldSource.MovedInto(laterMove) =>
              errsB += MigrationError(
                "Cannot move into a field that is also a split target",
                laterMove.span
              )
            // State all cases to fail compilation if a new source is added.
            case FieldSource.SplitInto(_, _) | FieldSource.Add(_) =>
              sys.error("unreachable: isPresent false and true")
          }

        case Right(_) | Left(CollectionFieldError.NotFound) => () // OK.

        case Left(CollectionFieldError.WildcardNeighbor) =>
          errsB += MigrationError(
            s"Cannot split field `${source.display}`, which is in a struct with a wildcard definition",
            source.span)
      }

      val b = Seq.newBuilder[(fql.ast.Path, Type, Option[Expr])]
      val union = ArraySeq.newBuilder[Type]

      def addErrMissing(p: fql.ast.Path): Unit = {
        // We don't have enough information here to guess the type of this
        // field, for a hint. The expected type is in the submitted schema,
        // which we don't have. Plus, migrations further in the list could
        // affect this type.
        errsB += MigrationError(
          s"Cannot split field, as `${p.display}` is not in the submitted schema",
          p.span)

        // Avoids frivolous errors.
        union += Type.Any
      }

      targets foreach { target =>
        shape.asImmutable.get(target) match {
          case Right(t) =>
            // The split declares `t`, so it must not have been redeclared later
            // by an add.
            t.source match {
              case FieldSource.Add(add) =>
                errsB += MigrationError(
                  "Cannot add a field that is also created by a split",
                  target.span,
                  Seq(
                    Hint("Field is added here", add.field.span),
                    Hint("Remove the add", add.span, Some(""))
                  )
                )
              case FieldSource.SplitInto(laterSplit, laterTarget) =>
                // This could be allowed, but isn't.
                errsB += MigrationError(
                  "Cannot split into a field that is already split into",
                  laterTarget.span,
                  Seq(
                    Hint("Field is also split into here", target.span),
                    Hint("Remove the later split", laterSplit.span, Some(""))
                  )
                )
              case FieldSource.MovedInto(laterMove) =>
                errsB += MigrationError(
                  "Cannot move into a field that is also a split target",
                  laterMove.span
                )
              case FieldSource.Split(_) | FieldSource.Schema(_) |
                  FieldSource.Drop(_) | FieldSource.Backfill(_) => // OK.
            }
            // `takeBackfill` will produce errors if `t` wasn't backfilled.
            val backfill = takeBackfill(t, target)
            union += t.ty
            shape.put(target, t.copy(source = FieldSource.SplitInto(spl, target)))
            b += ((target, t.ty, backfill))

          case Left(CollectionFieldError.NotFound) => addErrMissing(target)

          case Left(CollectionFieldError.WildcardNeighbor) =>
            errsB += MigrationError(
              s"Cannot split into field `${target.display}`, which is in a struct with a wildcard definition",
              target.span)
        }
      }
      addMigration(SchemaMigration.Split(source, b.result()))

      // Add a shape for the split-out-of field. Its type is the union of the types
      // split into, because the type of the field in the submitted schema must be
      // narrower than this so that all field values have a home in the outputs.
      // This will clobber the existing shape if the field was re-added.
      //
      // Additionally, if the parent of this field doesn't exist, we don't modify the
      // shape. This is an invalid state, and the compatability check will catch
      // this.
      val name = source.elems.last match {
        case PathElem.Field(name, span) => Name(name, span)
        case _ =>
          throw new IllegalStateException(
            "Last element in a migration path must be a field")
      }

      var valid = true
      var prefix = fql.ast.Path(Nil, Span.Null)
      for (elem <- source.elems.dropRight(1)) {
        prefix = prefix.copy(elems = prefix.elems :+ elem)
        shape.asImmutable.get(prefix) match {
          case Right(CollectionField(_, FieldValue.Record(_, _, _), _)) => ()
          case _ => valid = false
        }
      }

      if (valid) {
        // Widen the source field so it's nullable. The actual type of the field
        // may be narrower.
        union += Type.Null
        shape.put(
          source,
          CollectionField(
            name,
            FieldValue.Union(union.result(), Span.Null),
            source = FieldSource.Split(spl))
        )
      }

      ()
  }

  // Rewind an add migration. Adds declare fields, and so consume backfills.
  def rewindAdd(add: MigrationItem.Add): Unit = add match {
    case MigrationItem.Add(field, span) =>
      shape.asImmutable.get(field) match {
        case Right(f) =>
          f.source match {
            case FieldSource.SplitInto(split, target) =>
              errsB += MigrationError(
                s"Cannot split field: field `${field.display}` cannot both be added and be a split output",
                field.span,
                Seq(
                  Hint("Field is an output of a split", target.span),
                  Hint("Remove the add", span, Some("")),
                  Hint("Remove the split", split.span, Some(""))
                )
              )
            case FieldSource.Add(laterAdd) =>
              errsB += MigrationError(
                s"Cannot add field `${field.display}` twice",
                laterAdd.field.span,
                Seq(
                  Hint("Field added here", field.span),
                  Hint("Remove the extra add", laterAdd.span, Some(""))
                )
              )
            case FieldSource.MovedInto(move) =>
              errsB += MigrationError(
                s"Cannot move into field `${field.display}` as it was also added",
                move.span,
                Seq(
                  Hint("Field moved into here", move.to.span),
                  Hint("Remove the add", span, Some("")),
                  Hint("Remove the move", move.span, Some(""))
                )
              )
            case _ =>
              shape.wildcard match {
                case Some(wildcard) if field.path.elems.length == 1 =>
                  val backfill = takeBackfill(f, field)

                  currentMoveConflicts match {
                    case Some(_) =>
                      addMigration(SchemaMigration.Add(field, f.ty, backfill))

                    case None =>
                      if (
                        !CollectionShape.isSchemaSubtype(
                          typer,
                          Type.Union(wildcard.ty, Type.Null),
                          f.ty)
                      ) {
                        errsB += MigrationError(
                          s"Cannot add field, as `${field.display}` conflicts with the wildcard",
                          field.span,
                          Seq(
                            Hint(
                              "Add a `move_conflicts` migration",
                              add.span.copy(start = add.span.end),
                              Some("\n    move_conflicts .conflicts")
                            ))
                        )
                      }
                  }

                case _ =>
                  val backfill = takeBackfill(f, field)
                  addMigration(SchemaMigration.Add(field, f.ty, backfill))
              }

              shape.put(field, f.copy(source = FieldSource.Add(add)))
          }

        case Left(CollectionFieldError.NotFound) =>
          errsB += MigrationError(
            s"Cannot add field, as `${field.display}` is not in the submitted schema",
            field.span)
        case Left(CollectionFieldError.WildcardNeighbor) =>
          errsB += MigrationError(
            s"Cannot add field, as there is a wildcard in the same struct as `${field.display}`",
            field.span)
      }
  }

  def rewindMoveWildcardConflicts(m: MigrationItem.MoveWildcardConflicts): Unit = {
    shape.wildcard match {
      case Some(wildcard) if wildcard.isPresent =>
        shape.asImmutable.get(m.into) match {
          case Right(f) if f.isPresent =>
            val expected = Type.Union(Type.WildRecord(wildcard.ty), Type.Null)

            // This is a strict equality check to ensure that the type exactly
            // matches. The type of the conflicts field may only contain a wildcard,
            // no extra fields or anything.
            val isDrop = f.source match {
              case FieldSource.Drop(_) => true
              case _                   => false
            }
            if (f.ty != expected && !isDrop) {
              errsB += MigrationError(
                s"Wildcard conflicts field must have the type `${typer.displayValue(expected)}`",
                m.into.span)
            }

          case Right(_) | Left(CollectionFieldError.NotFound) =>
            errsB += MigrationError(
              s"Cannot move wildcard conflicts to field `${m.into.display}`, as it is not in the submitted schema",
              m.into.span)
          case Left(CollectionFieldError.WildcardNeighbor) =>
            errsB += MigrationError(
              s"Cannot move wildcard conflicts to field, as there is a wildcard in the same struct as `${m.into.display}`",
              m.into.span)
        }

        currentMoveConflicts = Some(m.into)
        addMigration(SchemaMigration.MoveWildcardConflicts(m.into))

      case _ =>
        errsB += MigrationError(
          s"Cannot move wildcard conflicts, as there is no wildcard in the submitted schema",
          m.into.span,
          Seq(
            Hint(
              "Add a wildcard constraint", {
                // The max should always be at the end.
                // Fields must be non-empty because empty implies wildcard.
                val a = target.fields.last.span.end
                target.span.copy(start = a, end = a)
              },
              // TODO: Fake the new line and indentation for now.
              //            We also have faked indentation in suggestMigration.
              //            Also, just guess the type is Any. Shrug.
              Some("\n  *: Any")
            ),
            suggestMigration(
              target.migrations,
              target.span
                .copy(start = target.span.end - 1, end = target.span.end - 1),
              "Move the extra fields into a conflicts field with `move_wildcard`",
              Seq(s"move_wildcard ${m.into.display}")
            )
          )
        )
    }
  }

  def rewindMoveWildcard(m: MigrationItem.MoveWildcard): Unit = {
    shape.wildcard match {
      case Some(wildcard) if wildcard.isPresent =>
        errsB += MigrationError(
          s"Cannot move wildcard, as there is still a wildcard in the submitted schema",
          m.into.span
        )

      case _ =>
        val wildcard = shape.asImmutable.get(m.into) match {
          case Right(f) if f.isPresent =>
            val expected = Type.Union(Type.WildRecord(Type.Any), Type.Null)

            def err() = {
              errsB += MigrationError(
                s"Wildcard conflicts field must have the type `${typer.displayValue(expected)}`",
                m.into.span)
            }

            // Pull out the wildcard of the struct.
            val isDrop = f.source match {
              case FieldSource.Drop(_) => true
              case _                   => false
            }
            f.ty match {
              // Match `{ *: A } | Null`.
              case Type.Union(
                    ArraySeq(Type.Record(fields, Some(wildcard), _), Type.Null),
                    _) =>
                if (fields.nonEmpty) err()
                wildcard

              // Match `Null | { *: A }`.
              case Type.Union(
                    ArraySeq(Type.Null, Type.Record(fields, Some(wildcard), _)),
                    _) =>
                if (fields.nonEmpty) err()
                wildcard

              case _ if isDrop => Type.Any

              case _ =>
                err()
                Type.Any
            }

          case Right(_) | Left(CollectionFieldError.NotFound) =>
            errsB += MigrationError(
              s"Cannot move wildcard conflicts to field `${m.into.display}`, as it is not in the submitted schema",
              m.into.span)
            Type.Any
          case Left(CollectionFieldError.WildcardNeighbor) =>
            errsB += MigrationError(
              s"Cannot move wildcard conflicts to field, as there is a wildcard in the same struct as `${m.into.display}`",
              m.into.span)
            Type.Any
        }

        addMigration(
          SchemaMigration.MoveWildcard(
            m.into,
            shape.fields.values.collect {
              case f if f.isPresent => f.name.str
            }.toSet))
        shape.wildcard = Some(
          CollectionWildcard(wildcard, WildcardSource.MoveWildcard, Span.Null))
    }
  }

  def rewindAddWildcard(m: MigrationItem.AddWildcard): Unit = shape.wildcard match {
    case Some(w) =>
      w.source match {
        case WildcardSource.Schema | WildcardSource.Implicit |
            WildcardSource.MoveWildcard =>
          shape.wildcard = Some(w.copy(source = WildcardSource.AddWildcard(m.span)))
          addMigration(SchemaMigration.AddWildcard)
        case WildcardSource.AddWildcard(span) =>
          errsB += MigrationError(
            "Cannot add a wildcard twice",
            span,
            Seq(
              Hint("Remove the duplicate `add_wildcard` migration", span, Some(""))))
      }
    case None =>
      // TODO: Improve the hint system so we can suggest adding a wildcard.
      errsB += MigrationError(
        "Cannot add wildcard, as the submitted schema lacks a wildcard",
        m.span)

  }

  // rewindOne rewinds a single migration, appending a corresponding internal
  // migration to the migrations of the growing path.
  private def rewindOne(migration: MigrationItem): Unit = {
    migration match {
      case bf: MigrationItem.Backfill             => rewindBackfill(bf)
      case d: MigrationItem.Drop                  => rewindDrop(d)
      case r: MigrationItem.Move                  => rewindMove(r)
      case spl: MigrationItem.Split               => rewindSplit(spl)
      case add: MigrationItem.Add                 => rewindAdd(add)
      case m: MigrationItem.MoveWildcardConflicts => rewindMoveWildcardConflicts(m)
      case m: MigrationItem.MoveWildcard          => rewindMoveWildcard(m)
      case m: MigrationItem.AddWildcard           => rewindAddWildcard(m)
    }
  }

  // Helper for the `split` migration. `split` stores the backfill values directly,
  // so we need to remove the explicit `Add` migrations the backfill statements
  // added.
  //
  // If `remove` is set, then it'll remove the previous `Add` and produce errors.
  // This can be set to `false` to avoid producing any side effects.
  //
  // `name` is just passed in for errors.
  private def takeBackfill(
    field: CollectionField,
    path: fql.ast.Path,
    remove: Boolean = true): Option[Expr] = {
    field.source match {
      // If the field comes directly from schema, it must have a default value or be
      // nullable. Alternatively, it can be backfilled, which the `Backfill` case
      // below.
      case FieldSource.Schema(default) =>
        if (remove && default.isEmpty && !typer.isSubtype(Type.Null, field.ty)) {
          errsB += MigrationError(
            s"Cannot declare non-nullable field `${path.display}`: it has no default and no backfill value",
            path.span,
            Seq(
              Hint(
                "Make the field nullable",
                field.ty.span.from(field.ty.span.end),
                Some(s"?")),
              Hint(
                "Add a default value to this field",
                field.ty.span.copy(start = field.ty.span.end),
                Some(" = <expr>")),
              suggestMigration(
                target.migrations,
                target.span
                  .copy(start = target.span.end - 1, end = target.span.end - 1),
                "Add a `backfill` migration",
                Seq(s"backfill ${path.display} = <expr>")
              )
            )
          )
        }
        default.map(_.expr)

      // The result of a `split`, `drop`, or `backfill` migration are all valid
      // inputs to a `split` migration.
      case FieldSource.Split(_) => None
      case FieldSource.Drop(_)  => None
      case FieldSource.Backfill(migration) =>
        Some(migration.value.expr)

      // TODO: Disallow
      case FieldSource.SplitInto(_, _) => None
      case FieldSource.Add(_)          => None
      case FieldSource.MovedInto(_)    => None
    }
  }

  // Validate the collection shape post-rewind.
  private def validateShape(ms: Seq[MigrationItem], shape: CollectionShape): Unit =
    // Check that no field is left in a backfill-pending-add-or-split state.
    // Iterating over the migrations and finding the field source that way is easier
    // than iterating over the shape.
    ms foreach {
      case MigrationItem.Backfill(path, _, span) =>
        shape.get(path) foreach { field =>
          field.source match {
            case FieldSource.Backfill(_) =>
              // NB: There should only be one (bad) backfill in this case. Otherwise,
              // there were multiple backfills. This would've been caught in rewind
              // because there were multiple backfills without adds or splits, or
              // there was an add or a split after the first backfill with none above
              // it, which would've been an error on rewind.
              // The point is I can use whatever span and I'm right :).
              // TODO: Suggest an add, but it needs to go above the backfill.
              //            Also, it's too bad the live shape isn't available here.
              errsB += MigrationError(
                s"Field `${path.display}` is backfilled without being declared by an `add` or `split` migration",
                span)
            case _ => // OK.
          }
        }
      case _ => // OK.
    }

  def result[T](ok: => T): Result[T] = {
    if (errsB.result().isEmpty) {
      Result.Ok(ok)
    } else {
      Result.Err(errsB.result())
    }
  }
}

/** Explains why a submitted schema is incompatible with a live schema.
  *
  * The given `coll` is the target schema, and is used for spans when producing
  * errors.
  */
class IncompatibilityExplainer(
  val typer: Typer,
  val target: CollectionShape,
  coll: SchemaItem.Collection) {
  val errsB = new ListBuffer[Error]

  def explain(live: CollectionShape): List[Error] = {
    live.fields.values.foreach { liveField =>
      val srcType = liveField.ty
      target.fields.get(liveField.name.str) match {
        case Some(targetField) if targetField.isRemoved =>
          errsB += MigrationError(
            s"Field `.${liveField.name.str}` is in the live schema, so it cannot be added or backfilled",
            targetField.name.span
          )

        case Some(CollectionField(_, _, FieldSource.Backfill(bf))) =>
          // Special case: a backfill that was never resolved by an add or split.
          errsB += MigrationError(
            s"Field `.${liveField.name.str}` is in the live schema, so it cannot be added or backfilled",
            bf.span
          )

        // `srcField` must be a supertype of `targetField`.
        case Some(targetField) if typer.isSubtype(srcType, targetField.ty) => ()

        case Some(targetField) =>
          // Because `srcType` wasn't a subtype of `f0.ty`, we can render the
          // leftover bit as the diff between the two for the error message.
          //
          // For example, if `srcType` is `Int` and `target.ty` is `Int | String`,
          // the
          // diff between these will be the type `String`. So we can add a
          // suggestion to split the `String`s into another field.
          val tmpStr = typer.displayValue(Constraint.Diff(srcType, targetField.ty))

          errsB += MigrationError(
            s"Field `.${liveField.name.str}` of type `${targetField.ty.display}` does not match the live type `${srcType.display}`",
            targetField.ty.span,
            Seq(
              Hint(
                "Add a migration to split the invalid types into another field",
                liveField.name.span.copy(end = liveField.name.span.start),
                // TODO: This is kinda hard to read, especially because its on
                // multiple lines. It'd be nice to consolidate this.
                Some(s"""|tmp: $tmpStr
                         |  migrations {
                         |    split .${liveField.name.str} -> .${liveField.name.str}, .tmp
                         |  }
                         |""".stripMargin)
              ))
          )

        case None =>
          errsB += MigrationError(
            s"Field `.${liveField.name.str}` is missing from the submitted schema",
            coll.span,
            Seq(
              MigrationValidator.suggestMigration(
                coll.migrations,
                // Suggest the migrations block at the end of the collection block.
                coll.span.copy(start = coll.span.end - 1, end = coll.span.end - 1),
                "Drop the field and its data with a `drop` migration",
                Seq(s"drop .${liveField.name.str}")
              ))
          )
      }
    }

    target.fields.values.view
      .filterNot { f => live.fields.contains(f.name.str) } // Covered by check above.
      .foreach { targetField =>
        targetField.source match {
          case FieldSource.Drop(_) if !live.fields.contains(targetField.name.str) =>
            // Oops. A field that didn't exist was dropped.
            errsB += MigrationError(
              s"Field `.${targetField.name.str}` was dropped but it doesn't exist in the live schema",
              targetField.name.span)
          case FieldSource.Backfill(bf) =>
            throw new IllegalStateException(
              s"Field ${bf.field.display} is backfill-pending but wasn't caught by shape validation")
          case FieldSource.Schema(default) =>
            val noBackfillRequired =
              typer.isSubtype(Type.Null, targetField.ty) || default.isDefined
            if (noBackfillRequired) {
              errsB += MigrationError(
                s"Field `.${targetField.name.str}` is not present in the live schema",
                targetField.name.span,
                Seq(
                  MigrationValidator.suggestMigration(
                    coll.migrations,
                    coll.span
                      .copy(start = coll.span.end - 1, end = coll.span.end - 1),
                    "Provide an `add` migration for this field",
                    Seq(s"add .${targetField.name.str}")
                  ))
              )
            } else {
              def go(
                prefix: Path,
                field: CollectionField): Seq[(Path, CollectionField)] = {
                val innerPath = new Path(
                  prefix.elems :+ PathElem.Field(field.name.str, Span.Null),
                  Span.Null)

                field.source match {
                  case FieldSource.Add(_) => Seq.empty
                  case FieldSource.Schema(_) =>
                    field.value match {
                      case FieldValue.Record(fs, hasWild, _) =>
                        if (hasWild) {
                          Seq.empty
                        } else {
                          fs.flatMap { case (_, f) => go(innerPath, f) }.toSeq
                        }

                      case _ => Seq(innerPath -> field)
                    }

                  case _ => Seq(innerPath -> field)
                }
              }

              val nonAddedFields = go(new Path(Nil, Span.Null), targetField)

              val (path, fieldMissingAdd) = {
                if (nonAddedFields.sizeIs == 1) {
                  nonAddedFields.iterator.next()
                } else {
                  val path =
                    new Path(
                      List(PathElem.Field(targetField.name.str, Span.Null)),
                      Span.Null)
                  (path, targetField)
                }
              }

              // TODO: We need to suggest the add migration and possibly
              // modifying
              // the field definition. Don't think Hint quite covers this.
              errsB += MigrationError(
                s"Field `${path.display}` is not present in the live schema",
                fieldMissingAdd.name.span,
                Seq(
                  Hint(
                    "Provide an `add` migration for this field",
                    fieldMissingAdd.name.span.copy(end =
                      fieldMissingAdd.name.span.start),
                    Some(s"""|migrations {
                           |    add .${fieldMissingAdd.name.str}
                           |    backfill .${fieldMissingAdd.name.str} = <expr>
                           |  }
                           |""".stripMargin)
                  ),
                  // TODO: Should also be suggesting an add migration.
                  Hint(
                    "Add a default value to this field",
                    fieldMissingAdd.ty.span.copy(start =
                      fieldMissingAdd.ty.span.end),
                    Some(" = <expr>")
                  ),
                  // TODO: Should also be suggesting an add migration.
                  Hint(
                    "Make the field nullable",
                    fieldMissingAdd.ty.span.copy(start =
                      fieldMissingAdd.ty.span.end),
                    Some("?")
                  )
                )
              )
            }
          case _ => // No comment at this time.
        }
      }

    // Wildcard comparison.
    if (target.wildcard.isEmpty && live.wildcard.isDefined) {
      // If the target lacks a wildcard and the live schema has one, then either
      // the target lost its implicit wildcard by adding defined fields, or the
      // target simply lacks a wildcard declaration that the submitted schema has.
      if (live.wildcard.get.isRemoved) {
        throw new IllegalStateException(
          "live schema lacks wildcard but is incompatible with target lacking wildcard")
      }

      val hints = Seq.newBuilder[Hint]
      // If the target wildcard is empty, there must be a field defined.
      val field = target.fields.values.head

      if (live.wildcard.get.source == WildcardSource.Implicit) {
        hints += Hint(
          "Collections with no fields defined allow arbitrary fields by default. To keep this behavior, add a wildcard declaration to the schema definition",
          field.name.span.copy(end = field.name.span.start),
          Some("*: Any\n")
        )
      } else {
        hints += Hint(
          "Add the wildcard back",
          field.name.span.copy(end = field.name.span.start),
          Some("*: Any\n")
        )

        hints += MigrationValidator.suggestMigration(
          coll.migrations,
          field.name.span.copy(end = field.name.span.start),
          "Move the extra fields with a `move_wildcard` migration",
          Seq("move_wildcard .conflicts")
        )
        hints += MigrationValidator.suggestMigration(
          coll.migrations,
          field.name.span.copy(end = field.name.span.start),
          "Drop the extra fields with a `move_wildcard` and `drop` migration",
          Seq("move_wildcard .conflicts", "drop .conflicts")
        )
      }

      errsB += MigrationError(
        s"Missing wildcard definition",
        coll.span,
        hints.result())
    }

    if (target.wildcard.nonEmpty && live.wildcard.isEmpty) {
      // A wildcard was added back, either explicitly or because all defined fields
      // were removed.
      val wc = target.wildcard.get
      wc.source match {
        case WildcardSource.MoveWildcard | WildcardSource.Schema =>
          errsB += MigrationError(
            "Wildcard was not present in the live schema",
            wc.span,
            Seq(
              suggestMigration(
                coll.migrations,
                coll.span.copy(start = coll.span.end - 1, end = coll.span.end - 1),
                "Add the wildcard with an `add_wildcard` migration",
                Seq("add_wildcard")))
          )
        case WildcardSource.Implicit =>
          // This is a lousy error, but this is a bizarre situation.
          errsB += MigrationError(
            "All fields from the live schema are dropped, reintroducing the implicit wildcard that requires an `add_wildcard` migration. Are you sure you meant to do this?",
            coll.span
          )
        case WildcardSource.AddWildcard(_) => // Compatible w.r.t. wildcards.
      }
    }

    val errs = errsB.result()
    if (errsB.isEmpty) {
      // This should never happen, but if it does, we can at least provide a
      // generic error.
      List(
        MigrationError(
          s"Submitted schema is not compatible with the live schema",
          coll.span))
    } else {
      errs
    }
  }
}
