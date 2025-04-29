package fauna.model.schema.migration

import fauna.atoms.ScopeID
import fauna.auth.EvalAuth
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure, Result }
import fauna.model.schema.SchemaTypeResolver
import fauna.repo.query.Query
import fauna.repo.schema.migration.Migration
import fauna.repo.schema.ConstraintFailure
import fauna.repo.schema.Path
import fauna.repo.schema.ScalarType
import fauna.repo.schema.SchemaType
import fauna.repo.values.Value
import fauna.storage.doc.ConcretePath
import fauna.storage.ir.IRValue
import fauna.storage.ir.NullV
import fql.ast.{ Expr, PathElem, Span }
import fql.migration.SchemaMigration
import fql.typer.Type
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

object MigrationConverter {
  def apply(
    scope: ScopeID,
    ignoreBackfillErrors: Boolean = false): Query[MigrationConverter] =
    Query.value(new MigrationConverter(scope, ignoreBackfillErrors))
}

class MigrationConverter(val scope: ScopeID, val ignoreBackfillErrors: Boolean) {

  val auth = EvalAuth(scope)

  implicit def pathToRepo(s: fql.ast.Path): Path = new Path(s.toList)

  private def concretePath(field: fql.ast.Path): Result[ConcretePath] =
    Result.Ok(
      ConcretePath("data").concat(
        ConcretePath(
          field.elems.view
            .map {
              case PathElem.Field(name, _) => name
              case PathElem.Index(_, _) =>
                throw new IllegalStateException(
                  "indexes in migration paths are not supported")
            }
            .to(ArraySeq))))

  def fromFSL(m: SchemaMigration): Query[Result[Seq[Migration]]] = {
    m match {
      case SchemaMigration.Add(field, expected, backfill) =>
        evalTypedOptional(backfill, expected, field).map {
          case Result.Err(_) if ignoreBackfillErrors =>
            concretePath(field).map { field =>
              // If we're ignoring backfill errors, we don't care about the
              // actual backfill value.
              Seq(Migration.AddField(field, ScalarType.Null, NullV))
            }

          case res =>
            for {
              res <- res
              (backfill, expected) = res
              field <- concretePath(field)
            } yield Seq(Migration.AddField(field, expected, backfill))
        }

      case SchemaMigration.Drop(field) =>
        Query.value(for {
          field <- concretePath(field)
        } yield Seq(Migration.DropField(field)))

      case SchemaMigration.Move(from, to) =>
        Query.value(for {
          from <- concretePath(from)
          to   <- concretePath(to)
        } yield Seq(Migration.MoveField(from, to)))

      case SchemaMigration.Split(source, targets) =>
        if (targets.size < 2) {
          // I'm throwing because we should've caught this multiple times before.
          throw new IllegalStateException(
            s"split has too few targets (${targets.size} < 2)")
        }
        val head = targets.head
        val b = Seq.newBuilder[Query[Result[Migration]]]

        // The first part of the split is special. If `firstTarget` is the same
        // field as `source`, it's a no-op. Otherwise, it's a move.
        if (source != head._1) {
          b += Query.value(for {
            from <- concretePath(source)
            to   <- concretePath(head._1)
          } yield Migration.MoveField(from, to))
        }

        targets
          .sliding(2)
          .foreach { pair =>
            val (from, fromTy, fromBackfill) = pair(0)
            val (to, _, toBackfill) = pair(1)
            val q = evalTypedOptional(fromBackfill, fromTy, from).flatMapT {
              case (replace, expected) =>
                evalOptional(toBackfill).map { backfill =>
                  for {
                    backfill <- backfill
                    from     <- concretePath(from)
                    to       <- concretePath(to)
                  } yield Migration.SplitField(from, to, expected, replace, backfill)
                }
            }
            b += q
          }

        b.result().sequenceT

      case SchemaMigration.MoveWildcardConflicts(into) =>
        Query.value(for {
          into <- concretePath(into)
        } yield Seq(Migration.MoveConflictingFields(into)))

      case SchemaMigration.MoveWildcard(into, fields) =>
        Query.value(
          for {
            into <- concretePath(into)
          } yield Seq(
            Migration
              .MoveWildcard(ConcretePath("data"), into, fields)))

      case SchemaMigration.AddWildcard =>
        // No internal migrations required.
        Query.value(Result.Ok(Seq.empty))
    }
  }

  private def evalTypedOptional(
    expr: Option[Expr],
    ty: Type,
    path: Path): Query[Result[(IRValue, SchemaType)]] =
    (evalOptional(expr), schemaType(ty, path)) parT { case (value, expected) =>
      checkValueAgainst(
        value,
        expected,
        expr.map { _.span }.getOrElse(ty.span),
        path).mapT { _ =>
        (value, expected)
      }
    }

  private def checkValueAgainst(
    value: IRValue,
    ty: SchemaType,
    exprSpan: Span,
    path: Path): Query[Result[Unit]] = {
    SchemaType
      .validate(ty, new Path.Prefix(path.elements.reverse), value, None)
      .map {
        case Seq() => Result.Ok(())

        case errs =>
          Result.Err(
            QueryRuntimeFailure.SchemaConstraintViolation(
              "Failed to convert backfill value.",
              errs,
              FQLInterpreter.StackTrace(Seq(exprSpan))
            ))
      }
  }

  private def schemaType(ty: Type, path: Path): Query[Result[SchemaType]] =
    SchemaTypeResolver(ty)(auth.scopeID).map {
      case Some(expected) => Result.Ok(expected)

      case None =>
        Result.Err(
          QueryRuntimeFailure.SchemaConstraintViolation(
            "Failed to convert migration.",
            Seq(ConstraintFailure
              .ValidatorFailure(path, "Type cannot be persisted.")),
            FQLInterpreter.StackTrace(Seq(ty.span))
          ))
    }

  private def evalOptional(expr: Option[Expr]): Query[Result[IRValue]] = {
    expr match {
      case Some(v) => evalPure(v)
      case None    => Query.value(Result.Ok(NullV))
    }
  }

  private def evalPure(expr: Expr): Query[Result[IRValue]] =
    FQLInterpreter(auth)
      .evalWithTypecheck(expr, Map.empty, FQLInterpreter.TypeMode.Disabled)
      .flatMap {
        case Result.Ok((v, _)) =>
          Value.toIR(v) match {
            case Left(e) =>
              Query.value(
                Result.Err(QueryRuntimeFailure.SchemaConstraintViolation(
                  "Failed to convert migration.",
                  Seq(ConstraintFailure.ValidatorFailure(
                    e.path,
                    s"Value has type ${e.provided.displayString}, which cannot be persisted.")),
                  FQLInterpreter.StackTrace(Seq(expr.span))
                )))
            case Right(v) => Query.value(Result.Ok(v))
          }

        case res @ Result.Err(_) => Query.value(res)
      }
}
