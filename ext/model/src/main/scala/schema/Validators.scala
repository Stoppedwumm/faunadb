package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.fql2.{
  PostEvalHook,
  QueryCheckFailure,
  QueryRuntimeFailure,
  Result
}
import fauna.model.schema.NamedCollectionID
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, FieldSchema, Path, WriteHook }
import fauna.repo.values.Value
import fauna.storage.doc.Data
import fauna.storage.ir.{ ArrayV, DocIDV, IRValue, LongV, MapV, StringV }
import fql.{ Result => FQLResult }
import fql.ast.{ Expr, Src }
import fql.parser.{ Parser, Tokens }
import java.net.URL
import scala.collection.mutable.{ Map => MMap }

object Validators {
  import FieldSchema._
  import Value.Func.Arity

  // FIXME: revert back to Tokens.isValidIdent once we have fixed _ handling
  // in parsing
  def isValidName(name: String) =
    Tokens.isValidIdent(name) && name != "_"

  // Field validators
  def LambdaParseValidator(arity: Arity): FieldValidator =
    LambdaParseValidator(Some(arity))
  def LambdaParseValidator(
    arity: Option[Arity] = None,
    allowShortLambda: Boolean = true): FieldValidator = {
    case (prefix, StringV(sourceStr), _) =>
      val msg = "Unable to parse FQL source code."
      val fieldName = prefix.elems.head.fold(_.toString, _.toString)
      val src = Src.Inline(s"*$fieldName*", sourceStr)

      val parsed =
        (Parser.lambdaExpr(sourceStr, src): Result[Expr.Lambda]).flatMap {
          case l: Expr.ShortLambda if !allowShortLambda =>
            Result.Err(QueryCheckFailure.InvalidShortLambda(l.span))

          case l => Result.Ok(l.params.length)
        }

      val errs = parsed match {
        case Result.Ok(lamArity) =>
          arity match {
            case Some(ar) if !ar.accepts(lamArity) =>
              Seq(ConstraintFailure.ValidatorFailure(
                prefix.toPath,
                s"Invalid function arity. Expected ${ar.displayString()}, but the function was defined with $lamArity arguments"))
            case _ => Seq.empty
          }

        case Result.Err(err: QueryCheckFailure) =>
          val description = err.errors.map {
            _.renderWithSource(Map.empty)
          } mkString "\n"
          Seq(
            ConstraintFailure
              .ValidatorFailure(prefix.toPath, s"$msg\n${description.trim()}"))
        case Result.Err(err: QueryRuntimeFailure) =>
          val desc = err.message
          Seq(ConstraintFailure.ValidatorFailure(prefix.toPath, s"$msg\n$desc"))
      }
      Query.value(errs)
    case _ => Query.value(Nil)
  }

  def ExprParseValidator: FieldValidator = {
    case (prefix, StringV(sourceStr), _) =>
      val msg = "Unable to parse FQL source code."
      val fieldName = prefix.elems.head.fold(_.toString, _.toString)
      val src = Src.Inline(s"*$fieldName*", sourceStr)

      val parsed = Parser.expr(sourceStr, src): Result[_]

      val errs = parsed match {
        case Result.Ok(_) => Seq.empty

        case Result.Err(err: QueryCheckFailure) =>
          val description = err.errors.map {
            _.renderWithSource(Map.empty)
          } mkString "\n"
          Seq(
            ConstraintFailure
              .ValidatorFailure(prefix.toPath, s"$msg\n${description.trim()}"))

        case Result.Err(err: QueryRuntimeFailure) =>
          val desc = err.message
          Seq(ConstraintFailure.ValidatorFailure(prefix.toPath, s"$msg\n$desc"))
      }

      Query.value(errs)
    case _ => Query.value(Seq.empty)
  }

  val OneOrMore: FieldValidator = {
    case (prefix, ArrayV(elems), _) if elems.isEmpty =>
      Query.value(
        Seq(
          ConstraintFailure
            .ValidatorFailure(prefix.toPath, "Expected 1 or more elements.")))

    case _ => Query.value(Nil)
  }

  val UserCollectionDoc: FieldValidator = {
    case (prefix, DocIDV(id), _) if UserCollectionID.unapply(id.collID).isEmpty =>
      Query.value(
        Seq(
          ConstraintFailure.ValidatorFailure(
            prefix.toPath,
            "Expected document from a user-defined collection."))
      )
    case _ => Query.value(Nil)
  }

  val UrlValidator: FieldValidator = {
    case (prefix, StringV(str), _) =>
      def err = Seq(
        ConstraintFailure
          .ValidatorFailure(prefix.toPath, s"`$str` must be a valid https URI."))

      try {
        val url = new URL(str)

        if (url.getProtocol.toLowerCase != "https") {
          Query.value(err)
        } else {
          Query.value(Nil)
        }
      } catch {
        case _: Exception =>
          Query.value(err)
      }

    case _ => Query.value(Nil)
  }

  val ValidIdentifierValidator: FieldValidator = {
    case (path, StringV(name), None) =>
      Query.value(if (!isValidName(name)) {
        Seq(ConstraintFailure.ValidatorFailure(path.toPath, "Invalid identifier."))
      } else {
        Seq.empty
      })
    case (path, StringV(name), Some(StringV(fromName))) =>
      Query.value(if (name != fromName && !isValidName(name)) {
        Seq(ConstraintFailure.ValidatorFailure(path.toPath, "Invalid identifier."))
      } else {
        Seq.empty
      })
    case (pathPrefix, toValue, fromValue) =>
      throw new IllegalStateException(
        s"Unexpected type for field $pathPrefix, expected StringV, received toValue: $toValue, fromValue: $fromValue")
  }

  val RoleValidator: FieldValidator = {
    case (path, StringV(name), _) =>
      Query.value(if (!isValidName(name)) {
        Seq(ConstraintFailure.ValidatorFailure(path.toPath, "Invalid identifier."))
      } else if (name == "admin" || name == "server") {
        Seq(
          ConstraintFailure
            .ValidatorFailure(path.toPath, s"The identifier `$name` is reserved."))
      } else {
        Seq.empty
      })
    case (pathPrefix, toValue, fromValue) =>
      throw new IllegalStateException(
        s"Unexpected type for field $pathPrefix, expected StringV, received toValue: $toValue, fromValue: $fromValue")
  }

  val AccessProviderRolesValidator: FieldValidator = {
    case (path, ArrayV(roles), _) =>
      val seenRoles = MMap[String, Boolean]()
      val constraintFailures =
        Seq.newBuilder[ConstraintFailure.FieldConstraintFailure]

      def checkRole(v: String): Unit = {
        // When this is true we have already logged a constraint failure for
        // it.
        if (seenRoles.get(v).contains(false)) {
          constraintFailures.addOne(
            ConstraintFailure
              .ValidatorFailure(
                path.toPath,
                s"The role `$v` is present more than once, there can only be one entry per role.")
          )
          seenRoles.put(v, true)
        } else {
          seenRoles.put(v, false)
        }

        v match {
          case Key.Role.Builtin(_) =>
            constraintFailures.addOne(
              ConstraintFailure.ValidatorFailure(
                path.toPath,
                s"Builtin role `$v` not allowed."
              )
            )

          case _ =>
        }
      }

      roles.foreach {
        case rm: MapV =>
          rm.get(List("role")).foreach {
            case StringV(v) => checkRole(v)
            case _          =>
          }
        case StringV(v) => checkRole(v)
        case _          =>
      }
      Query.value(constraintFailures.result())
    case _ => Query.value(Seq.empty)
  }

  val NonNegativeValidator: FieldValidator = {
    case (path, LongV(n), _) =>
      Query.value(if (n < 0) {
        Seq(
          ConstraintFailure
            .ValidatorFailure(path.toPath, "Non-negative value required."))
      } else {
        Nil
      })
    case _ => Query.value(Nil)
  }

  val UserSigValidator: FieldValidator = {
    case (path, StringV(sig), _) =>
      Query.value({
        val fieldName = path.elems.head.fold(_.toString, _.toString)
        val src = Src.Inline(s"*$fieldName*", sig)
        Parser.typeExpr(sig, src) match {
          case FQLResult.Ok(_)     => Nil
          case FQLResult.Err(errs) =>
            // The source used was an inline source, so we don't need a source ctx
            // here.
            val rendered = errs.map(_.renderWithSource(Map.empty)).mkString("\n")
            Seq(
              ConstraintFailure.ValidatorFailure(
                path.toPath,
                s"Failed parsing user-provided signature.\n$rendered"))
        }
      })
    case _ => Query.value(Nil)
  }

  val SchemaTypeExprValidator: FieldValidator = {
    case (path, StringV(sig), _) =>
      Query.value({
        val fieldName = path.elems.head.fold(_.toString, _.toString)
        val src = Src.Inline(s"*$fieldName*", sig)
        Parser.schemaTypeExpr(sig, src) match {
          case FQLResult.Ok(_)     => Nil
          case FQLResult.Err(errs) =>
            // The source used was an inline source, so we don't need a source ctx
            // here.
            val rendered = errs.map(_.renderWithSource(Map.empty)).mkString("\n")
            Seq(
              ConstraintFailure.ValidatorFailure(
                path.toPath,
                s"Failed parsing user-provided signature.\n$rendered"))
        }
      })
    case _ => Query.value(Nil)
  }

  // Revalidation Hooks

  /** Traverse `IRValue` types, applying the given function to the `IRValue` a the
    * end of the specified path. Note that, if a sub-path hits an `ArrayV` value,
    * it traverses all its elements.
    */
  private[Validators] def traverse[A](path: List[String], value: IRValue)(
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

  /** Check the given field paths for valid global names: native/user collections,
    * user-defined functions.
    *
    * When executed, this hook traverses the document's data looking for the
    * corresponding value at each given field path. If a literal string, it validates
    * it matches with a valid global name.
    *
    * Note that this hook presumes the shape of the document's data is correct, thus
    * silently ignoring non-literal string values or other non-traversable values
    * found in a given field path.
    */
  object NameValidationHook {

    def apply(field: Field, fields: Field*) =
      PostEvalHook.Revalidation("NameValidation") { (_, config, ev) =>
        (field +: fields).flatMap {
          validate(config, _, ev.newData.getOrElse(Data.empty).fields)
        }.sequence map {
          _.view.flatten.toSeq
        }
      }

    /** Possible type a given field can refer to by name. */
    sealed abstract class FieldType(
      override val toString: String,
      id: IDCompanion[_]) {
      def isValid(scope: ScopeID, name: String): Query[Boolean] = {
        // We aren't using this ID for anything, we just check if it exists. So we
        // don't care about the generic type `I`. Additionally, `I` is invariant, so
        // we need this cast (which does nothing at runtime).
        implicit val companion: IDCompanion[Nothing] =
          id.asInstanceOf[IDCompanion[Nothing]]

        SchemaNames
          .idByNameStagedUncached[Nothing](scope, name)
          .orElseT {
            id.collID match {
              case NamedCollectionID.AliasedCollectionID(_) =>
                SchemaNames.idByAliasStagedUncached[Nothing](scope, name)
              case _ => Query.none
            }
          }
          .isDefinedT
      }
    }
    object FieldType {
      case object Database extends FieldType("database", DatabaseID)
      case object UserFunction extends FieldType("function", UserFunctionID)
      case object Collection extends FieldType("collection", CollectionID) {
        override def isValid(scope: ScopeID, name: String) =
          name match {
            case NativeCollection(_) => Query.True
            case _                   => super.isValid(scope, name)
          }
      }
      case object Role extends FieldType("role", RoleID) {
        override def isValid(scope: ScopeID, name: String) =
          name match {
            case Key.Role.Builtin(_) => Query.True
            case _                   => super.isValid(scope, name)
          }
      }
    }

    /** A field to be validated to refer to one of the given types by name. */
    final class Field private (val types: Seq[FieldType], val path: List[String])
    object Field {
      def apply(tpe: FieldType, types: FieldType*)(
        path: String,
        paths: String*): Field =
        new Field(tpe +: types, path :: paths.toList)
    }

    private def validate(
      config: CollectionConfig,
      field: Field,
      value: IRValue
    ): Seq[Query[Option[ConstraintFailure]]] =
      traverse(field.path, value) { case (path, StringV(name)) =>
        isNameValid(config.parentScopeID, name, field.types) map { valid =>
          Option.when(!valid) {
            formatMessage(path, field.types, name)
          }
        }
      }

    private def isNameValid(
      scope: ScopeID,
      name: String,
      types: Seq[FieldType]
    ): Query[Boolean] =
      types.headOption match {
        case None => Query.False
        case Some(tpe) =>
          tpe.isValid(scope, name) flatMap {
            case true  => Query.True
            case false => isNameValid(scope, name, types.tail)
          }
      }

    private def formatTypes(types: Seq[FieldType]): String = {
      val end = if (types.sizeIs > 1) ", or " else ""
      val res = types.init.mkString("", ", ", end)
      res + types.last
    }

    def formatMessage(path: Path, types: Seq[FieldType], name: String) = {
      val fmt = formatTypes(types)
      val msg = s"Field refers to an unknown $fmt name `$name`."
      ConstraintFailure.ValidatorFailure(path, msg)
    }
  }

  /** Validate foreign references to a document based on the given source and target
    * field paths. This validation hook fails if foreign references are found during
    * delete operations.  Update operations are handled via a WriteHook that will
    * propagate the necessary updates.
    *
    * Foreign references are described as follows: on the source document, declare
    * fields that other documents may use to refer to the document via the `Src`
    * ctor. The `Src` ctor requires a list of possible foreign references described
    * by their `Location` and target field path. For example:
    *
    * {{
    *   ...
    *   revalidationhook = Some {
    *     FKValidationHook(
    *       Src("name")(Target(Location.UserFunction)("role"))
    *     )
    *   }
    *   ...
    * }}
    *
    * The above validation hook ensures that there are no functions with a "role"
    * field referring to the current document's "name" value before deleting the document.
    */
  object FKValidationHook {

    def apply(srcs: ForeignKey.Src*) =
      PostEvalHook.Revalidation("ForeignKeys") { (_, config, ev) =>
        ev match {
          case WriteHook.OnDelete(_, _) =>
            val validations =
              for {
                diff                     <- ev.diffOpt.toSeq
                src                      <- srcs
                prev @ StringV(prevName) <- diff.fields.get(src.path).toSeq
                target                   <- src.targets
              } yield {
                lazy val path = Path(src.path.map { Right(_) }: _*)
                target.loc.refersTo(
                  config.parentScopeID,
                  target.path,
                  ev.id,
                  prev) mapT { fkName =>
                  ConstraintFailure.ValidatorFailure(
                    path,
                    s"The ${target.loc} `$fkName` refers to this document by its previous identifier `$prevName`."
                  )
                }
              }

            validations.sequence map { _.flatten }
          case _ => Query.value(Seq.empty)
        }
      }
  }
}
