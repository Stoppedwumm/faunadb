package fql.env

import fql.ast._
import fql.error._
import fql.typer._
import fql.Result
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.collection.mutable.ListBuffer

/** This uses a Typer and a schema definition to validate a user's schema.
  * This is called as a post-eval hook if schema is enabled.
  *
  * The typer passed in should contain only the standard library.
  *
  * This is a one-shot class, that should be used to validate database schema once.
  */
final class SchemaTypeValidator(
  val stdlib: TypeEnv,
  val schema: DatabaseSchema,
  val isEnvTypechecked: Boolean) {

  val errs = new ListBuffer[Error]
  val builder = Typer.newEnvBuilder
  val typer = stdlib.newTyper()

  type CollInfo = Map[String, CollectionTypeInfo.Checked]
  type FunInfo = Map[String, TypeExpr.Scheme]
  type CheckedResult = (CollInfo, FunInfo, TypeEnv)

  def checkedEnv(): CheckedResult = {
    val colls = checkCollections()
    // NB: Check functions after collections, so that name conflicts prioritize
    // collections.
    val funs = checkFunctions()

    checkV4Functions()

    typer.typeEnv(builder) match {
      case Result.Ok(env) =>
        val checkedColls = colls.view.mapValues(_.getChecked()).toMap
        val checkedFuns = funs.view.mapValues(_.get).toMap
        (checkedColls, checkedFuns, env)

      case Result.Err(e) =>
        errs ++= e
        (Map.empty, Map.empty, stdlib)
    }
  }

  // This typer has empty type shapes for all the functions and collections in
  // `schema`. Used for checking signatures and persistability
  private val stubTyper = ModelTyper.stubTyper(stdlib.newTyper(), schema)

  private def checkTypeExpr(te: TypeExpr) =
    stubTyper.typeTExprType(te, allowGenerics = false, allowVariables = false)

  private def checkPersistability(ty: Type) =
    stubTyper.checkPersistability(ty)

  private def checkCollections(): Map[String, CollectionTypeInfo.Precheck] =
    schema.colls.flatMap { c =>
      val docType = Type.Named(c.name, Span.Null)

      val definedFields = checkDefinedFields(c)
      val computedFields = if (isEnvTypechecked) {
        checkComputedFields(c, docType)
      } else {
        SeqMap.empty[String, (Type, EnvCheckResult)]
      }

      val wildcard = checkWildcard(c)

      // Typecheck constraints and indexes
      if (isEnvTypechecked) {
        checkCheckConstraints(c, docType)
        checkIndexes(c, docType)
      }

      // If there are errors, we cannot build a doc type.
      if (errs.isEmpty) {
        val coll = CollectionTypeInfo.Precheck.fromSchemaItem(
          c,
          definedFields,
          computedFields,
          wildcard)

        if (isEnvTypechecked) {
          coll.allShapes.foreach { case (n, ts) => builder.addTypeShape(n, ts) }

          builder.addGlobal(
            coll.docName.str,
            Some(TypeExpr.Id(coll.collName.str, coll.collName.span)),
            None)

          // also add the aliased shapes for typechecking
          coll.aliased.foreach { aliased =>
            aliased.allShapes.foreach { case (n, ts) => builder.addTypeShape(n, ts) }

            builder.addGlobal(
              aliased.docName.str,
              Some(TypeExpr.Id(aliased.collName.str, aliased.collName.span)),
              None)
          }
        }

        Some(c.name.str -> coll)
      } else {
        None
      }
    }.toMap

  private def checkDefinedFields(c: SchemaItem.Collection)
    : SeqMap[String, (Either[Type, SchemaTypeExpr], Boolean)] = {
    c.fields.view
      .collect { case field: Field.Defined =>
        val te = field.schemaType

        val ret =
          checkTypeExpr(te.asTypeExpr) match {
            case fql.Result.Ok(ty) =>
              val pErrs = checkPersistability(ty)
              if (pErrs.nonEmpty) {
                errs += TypeError(
                  s"Field `${field.name.str}` in collection ${c.name.str} is not persistable",
                  te.span,
                  pErrs)
              } else {
                // Typecheck the default expression using the signature.
                field.value.map { expr =>
                  builder.addTypeCheck(ty, expr.value)
                }
              }

              Right(te)
            case fql.Result.Err(e) =>
              errs ++= e
              val v = typer.freshVar(te.span)(Type.Level.Zero)
              v.poison()
              Left(v)
          }

        field.name.str -> ((ret, field.value.isDefined))
      }
      .to(SeqMap)
  }

  private def checkComputedFields(c: SchemaItem.Collection, docType: Type) = {
    c.fields.view
      .collect { case field: Field.Computed =>
        val bodyExpr = field._value.expr

        val ret = field.ty match {
          case Some(sig) =>
            checkTypeExpr(sig) match {
              case fql.Result.Ok(ty) => ty
              case fql.Result.Err(e) =>
                errs ++= e
                val v = typer.freshVar(sig.span)(Type.Level.Zero)
                v.poison()
                v
            }

          case None => typer.freshVar(Span.Null)(Type.Level.Zero)
        }

        val thunk = builder.addTypeCheck(
          Type.Function(
            ArraySeq(None -> docType),
            None,
            ret,
            Span.Null
          ),
          bodyExpr
        )

        field.name.str -> (ret, thunk)
      }
      .to(SeqMap)
  }

  private def checkWildcard(c: SchemaItem.Collection) = {
    c.fields.view
      .collectFirst { case w: Field.Wildcard => w }
      .map { w =>
        val te = w._ty
        te match {
          case TypeExpr.Any(_) =>
            Right(te)
          case _ =>
            errs += TypeError("Top-level wildcard must have type `Any`", te.span)
            val v = typer.freshVar(te.span)(Type.Level.Zero)
            v.poison()
            Left(v)
        }
      }
  }

  private def checkCheckConstraints(c: SchemaItem.Collection, docType: Type) = {
    c.checks.foreach { constraint =>
      builder.addTypeCheck(
        Type.Function(
          ArraySeq(None -> docType),
          None,
          Type.Union(Type.Boolean, Type.Null),
          Span.Null
        ),
        constraint.member.config.expr
      )
    }
  }

  private def checkIndexes(c: SchemaItem.Collection, docType: Type) = {
    // Typecheck index paths vs. document type by parsing the
    // path into a short lambda.
    // Does not do the typecheck if the path is invalid.
    def addFieldTypecheck(docType: Type, path: Path, span: Span) = {
      val expr =
        Expr.ShortLambda(
          Expr.MethodChain(
            Expr.This(span),
            path.elems.map {
              case PathElem.Index(index, sp) =>
                Expr.MethodChain
                  .Access(Seq(Expr.Lit(Literal.Int(index), sp)), None, sp)

              // NB: If you select a non-identifier here, the AST will be a bit
              // messed
              // up. It'll still work as expected, and we never render this AST, so
              // its
              // probably fine.
              case PathElem.Field(name, sp) =>
                Expr.MethodChain.Select(sp, Name(name, sp), false)
            },
            span
          ))

      builder.addTypeCheck(
        Type.Function(
          ArraySeq(None -> docType),
          None,
          Type.Any,
          Span.Null
        ),
        expr
      )
    }

    c.indexes.foreach { idx =>
      idx.configValue.terms.foreach { t =>
        t.configValue.foreach { term =>
          addFieldTypecheck(docType, term.path, term.span)
        }
      }
      idx.configValue.values.foreach { v =>
        v.configValue.foreach { value =>
          addFieldTypecheck(docType, value.path, value.span)
        }
      }
    }

    c.uniques.foreach { unique =>
      unique.configValue.foreach { term =>
        addFieldTypecheck(docType, term.path, term.span)
      }
    }
  }

  private def checkFunctions(): Map[String, EnvCheckResult] = {
    if (!isEnvTypechecked) {
      return Map.empty
    }

    schema.funcs
      .filter { f => !builder.hasGlobal(f.name.str) }
      .map { func =>
        val variadic = func.sig.args.lastOption.exists(_.variadic)

        val argNames = func.sig.args.map { arg =>
          Option.when(arg.name.str != "_") { arg.name }
        }

        val lambda =
          Expr.LongLambda(
            if (variadic) argNames.init else argNames,
            Option.when(variadic) { argNames.last },
            func.body,
            func.span)

        val sig = if (func.sig.ret.isEmpty) {
          None
        } else {
          // Prior validation checks that the entire signature is defined or not
          // defined. So since the return is defined, we can assume all the params
          // have
          // types as well.

          val args = argNames.zip(func.sig.args.map { _.ty.get })

          Some(
            TypeExpr.Lambda(
              if (variadic) args.init else args,
              Option.when(variadic) {
                val arg = func.sig.args.last
                Some(arg.name) -> arg.ty.get
              },
              func.sig.ret.get,
              func.sig.span
            ))
        }

        // N.B. In the env checker, user functions are not typed as
        // `UserFunction<$sig>`, meaning calling `.definition` on a user func in
        // FSL will fail. However this behavior is deprecated, so no point in
        // allowing it in FSL just to take it away.
        func.name.str -> builder.addGlobal(func.name.str, sig, Some(lambda))
      }
      .toMap
  }

  private def checkV4Functions() =
    if (isEnvTypechecked) {
      schema.v4Funcs
        .filter { f => !builder.hasGlobal(f) }
        .foreach { func =>
          // v4 functions get typechecked as `Any`.
          builder.addGlobal(func, Some(TypeExpr.Any(Span.Null)), None)
        }
    }

  /** Check whether computed fields used in index terms and values are valid.
    * Indexed computed fields are not allowed to
    * 1. Use UDFs.
    * 2. Use other computed fields.
    * 3. Read or write.
    * Presently we can statically detect 1 and 2. All three restrictions are
    * also enforced at runtime.
    */
  def postcheckIndexedComputedFields(completeEnv: TypeEnv) = {
    val completeTyper = completeEnv.newTyper()
    completeTyper.recordingSpans = true
    schema.colls.foreach { c =>
      // Collect all computed fields used as terms or values in indexes
      // or unique constraints.

      // This is the first element of every index and unique constraint path.
      val indexedNames: Seq[String] = c.indexes.flatMap { idx =>
        (idx.configValue.terms match {
          case Some(terms) => terms.configValue.flatMap { _.path.head.asField }
          case None        => Seq.empty
        }) ++ (idx.configValue.values match {
          case Some(values) => values.configValue.flatMap { _.path.head.asField }
          case None         => Seq.empty
        })
      } ++ c.uniques.flatMap { idx =>
        idx.configValue.flatMap { _.path.head.asField }
      }

      val cfsByName = c.fields.collect { case f: Field.Computed =>
        f.name.str -> f
      }.toMap

      // Now find which of those fields are computed fields.
      val indexedCFields: Seq[Field.Computed] = indexedNames.flatMap { name =>
        cfsByName.get(name)
      }

      indexedCFields.foreach { case cf =>
        val lambdaExpr = cf._value.expr
        val free = lambdaExpr.freeVars

        // Check for UDFs: does a UDF name occurs as a free var in the
        // computed field body?
        schema.funcs.foreach { func =>
          if (free.contains(func.name.str)) {
            errs += TypeError(
              "Computed fields used in indexes or constraints cannot call UDFs",
              free.sites.get(func.name.str).fold(Span.Null) { _.head })
          }
        }

        // Check for collections: does a collection name occur as a free var in the
        // computed field body?
        schema.colls.foreach {
          case coll if free.contains(coll.name.str) =>
            errs += TypeError(
              "Computed fields used in indexes or constraints cannot access collections",
              free.sites.get(coll.name.str).fold(Span.Null) { _.head })
          case _ => ()
        }

        // Check for invalid field references: type the body, then see if any
        // method
        // chain contains a select where the LHS is a collection type and
        // the field is forbidden to reference. Forbidden fields are:
        // * Computed fields.
        // * The ts field.
        // Mild hack: type the body with the argument in the closure.
        val arg =
          if (lambdaExpr.params.isEmpty || lambdaExpr.params.head.isEmpty) {
            Map.empty[String, TypeScheme]
          } else {
            Map(lambdaExpr.params.head.get.str -> Type.Named(c.name.str).typescheme)
          }

        // Already typechecked; ignore the result. This just records spans.
        completeTyper.typeExpr(lambdaExpr.body, arg)

        def checkSelect(te: TypeExpr, field: Name) = {
          te match {
            case TypeExpr.Id(lhsName, _) =>
              // Check if any computed fields on docs are accessed.
              schema.colls.find(_.name.str == lhsName).map { lhsColl =>
                if (
                  lhsColl.fields.view.collect {
                    case f: Field.Computed if f.name.str == field.str =>
                  }.nonEmpty
                ) {
                  errs += TypeError(
                    "Computed fields used in indexes or constraints cannot use computed fields",
                    field.span)
                }

                if (field.str == "ts") {
                  errs += TypeError(
                    "Computed fields used in indexes or constraints cannot use a doc's ts field",
                    field.span
                  )
                }
              }

            case _ => ()
          }
        }

        lambdaExpr.visit { case (Expr.MethodChain(e, chain, _), cont) =>
          chain.foldLeft(completeTyper.typeAt(e.span)) { case (lhsTy, comp) =>
            comp match {
              case Expr.MethodChain.Select(_, field, _) =>
                lhsTy.foreach { ty =>
                  checkSelect(completeTyper.valueToTypeExpr(ty), field)
                }
              case Expr.MethodChain.MethodCall(_, field, _, _, _, _) =>
                lhsTy.foreach { ty =>
                  checkSelect(completeTyper.valueToTypeExpr(ty), field)
                }

              case _ => ()
            }
            completeTyper.typeAt(comp.span)
          }
          cont()
        }
      }
    }
  }

  def checkResourceName(name: Name, allowFunctions: Boolean) = {
    name.str match {
      // Public collections and schema collections.
      //
      // NB: `Credentials` is a legacy alias for `Credential`.
      case "Key" | "Token" | "Credential" | "Credentials" | "Database" |
          "Collection" | "Function" | "Role" | "AccessProvider" =>
        ()
      case n if schema.colls.exists { c =>
            c.name.str == n || c.alias.exists(_.configValue.str == n)
          } =>
        ()
      case n if allowFunctions && schema.funcs.exists { f =>
            f.name.str == n || f.alias.exists(_.configValue.str == n)
          } =>
        ()
      case n if allowFunctions && schema.v4Funcs.contains(n) => ()
      case _ =>
        errs += TypeError(s"Resource `${name.str}` does not exist", name.span)
    }
  }

  def checkAPRoleName(name: Name) = {
    name.str match {
      case "client" | "server" | "admin" | "server-readonly" =>
        errs += TypeError(s"Builtin role `${name.str}` is not allowed", name.span)

      case n if schema.roles.exists(_.name.str == n) => ()
      case n if schema.v4Roles.contains(n)           => ()
      case _ => errs += TypeError(s"Role `${name.str}` does not exist", name.span)
    }
  }
}

object SchemaTypeValidator {

  /** This is the main entry point to the SchemaTypeValidator. It accepts the
    * standard library, a database schema, and the state of the 'typechecked' flag on
    * the database.
    */
  def validate(
    stdlib: TypeEnv,
    schema: DatabaseSchema,
    isEnvTypechecked: Boolean): Result[TypecheckedEnvironment] = {

    val validator =
      new SchemaTypeValidator(stdlib, schema, isEnvTypechecked)

    val (colls, funcChecks, env) = validator.checkedEnv()

    if (validator.isEnvTypechecked) {
      validator.postcheckIndexedComputedFields(env)
    }

    val curErrs = validator.errs.result()

    val resErrs = if (curErrs.isEmpty) {
      // FIXME: These types should all be validated against the lambda type for the
      // given privilege. For example, the `read` predicates should get validated
      // against the type Doc => Boolean, and `write` predicates should get validated
      // against (Doc, Doc) => Boolean.
      val typer = env.newTyper()

      schema.roles.foreach { role =>
        role.membership.foreach { membership =>
          validator.checkResourceName(membership.name, allowFunctions = false)

          if (isEnvTypechecked) {
            membership.configValue.foreach { expr =>
              validator.errs ++= typer.typeExpr(expr).errOrElse(Nil)
            }
          }
        }

        role.privileges.foreach { p =>
          validator.checkResourceName(p.name, allowFunctions = true)

          if (isEnvTypechecked) {
            p.configValue.actions.foreach { action =>
              action.configValue.foreach { expr =>
                validator.errs ++= typer.typeExpr(expr).errOrElse(Nil)
              }
            }
          }
        }
      }

      schema.aps.foreach { ap =>
        ap.roles.foreach { role =>
          validator.checkAPRoleName(role.name)

          if (isEnvTypechecked) {
            role.configValue.foreach { expr =>
              validator.errs ++= typer.typeExpr(expr).errOrElse(Nil)
            }
          }
        }
      }

      validator.errs.result()
    } else {
      curErrs
    }

    if (resErrs.isEmpty) {
      Result.Ok(new TypecheckedEnvironment(stdlib, colls, funcChecks))
    } else {
      Result.Err(resErrs)
    }
  }
}
