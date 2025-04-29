package fql.analyzer

import fql.ast.{ Expr, FSL, Name, Path, PathElem, Span, Src, TypeExpr }
import fql.ast.display._
import fql.error.EmptyEmitter
import fql.parser.Parser
import fql.parser.TypeExprParser
import fql.typer.{ Type, TypeScheme, TypeShape, Typer }
import fql.Result
import scala.collection.mutable.{ Map => MMap }
import scala.collection.SeqMap

trait SchemaContextCompletions {
  self: SchemaContext =>

  def completionsImpl(cursor: Int, identSpan: Span): Seq[CompletionItem] = {
    ast match {
      // If there are diagnostics, we assume the tree may be invalid, and contain
      // some number of `Raw.Invalid` items. Therefore, we reparse and insert an
      // identifier at the cursor.
      //
      // This helps in cases where you have an invalid expr, which has fallen back to
      // `invalidChars` on the first pass. Inserting `faunaRulezz` at the cursor will
      // fix the syntax here, so we want to re-parse. For example:
      // ```
      // function foo() {
      //   Time.|
      // }
      // ```
      case Some(ast) if diagnostics.isEmpty =>
        walk(ast, walkTopLevel(_)(cursor, identSpan))(cursor, identSpan)
      case _ =>
        val modified = query.patch(cursor, "faunaRulezz", 0)
        Parser.fslNodesForCompletions(modified) match {
          case Result.Ok(ast) =>
            // if this worked, walk this new ast.
            walk(ast, walkTopLevel(_)(cursor, identSpan))(cursor, identSpan)

          case Result.Err(_) =>
            TopLevelCompletions(identSpan)
        }
    }
  }

  def snippetCompletion(label: String, detail: String, snippet: String)(
    implicit span: Span) = CompletionItem(
    label,
    detail,
    span,
    replaceText = "",
    newCursor = 0,
    kind = CompletionKind.Module,
    retrigger = false,
    snippet = Some(snippet))

  def TopLevelCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "collection",
        "A collection definition",
        s"""|collection $${name} {
            |\t$${0}
            |}""".stripMargin),
      snippetCompletion(
        "function",
        "A function definition",
        s"""|function $${name}($${param}: $${type}) {
            |\t$${0}
            |}""".stripMargin),
      snippetCompletion(
        "role",
        "A role definition",
        s"""|role $${name} {
            |\t$${0}
            |}""".stripMargin),
      snippetCompletion(
        "access provider",
        "An access provider definition",
        s"""|access provider $${name} {
            |\tissuer "$${issuer}"
            |\tjwks_uri "$${jwks_uri}"
            |}""".stripMargin
      )
    )

  def CollectionCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "index",
        "An index definition",
        s"""|index $${name} {
            |\t$${0}
            |}""".stripMargin),
      snippetCompletion(
        "unique",
        "A unique constraint definition",
        s"unique [.$${field}]"),
      snippetCompletion(
        "check",
        "A check constraint definition",
        s"check $${name} ($${predicate})"),
      snippetCompletion(
        "compute",
        "A computed field definition",
        s"compute $${name} = ($${lambda})"),
      snippetCompletion(
        "history_days",
        "Sets the history days of this collection",
        s"history_days $${number}"),
      snippetCompletion(
        "ttl_days",
        "Sets the default ttl for documents in this collection",
        s"ttl_days $${number}"),
      snippetCompletion(
        "migrations",
        "Add a field migration",
        s"""|migrations {
            |\t$${0}
            |}""".stripMargin)
    )

  def MigrationCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion("add", "Add a field", s"add .$${field}")
        .copy(retrigger = true),
      snippetCompletion(
        "backfill",
        "Backfill a field",
        s"backfill .$${field} = $${value}")
        .copy(retrigger = true),
      snippetCompletion("drop", "Drop a field", s"drop .$${field}")
        .copy(retrigger = true),
      snippetCompletion(
        "split",
        "Split a field into multiple new fields",
        s"split .$${from} -> .$${to}")
        .copy(retrigger = true),
      snippetCompletion("move", "Move a field", s"move .$${from} -> .$${to}")
        .copy(retrigger = true),
      snippetCompletion(
        "move_conflicts",
        "Move values that conflict with a newly defined field into a catch-all field",
        s"move_conflicts .$${field}")
        .copy(retrigger = true),
      snippetCompletion(
        "move_wildcard",
        "Move fields without a field definition into a catch-all field",
        s"move_wildcard .$${field}")
        .copy(retrigger = true)
    )

  def IndexCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "terms",
        "Defines the terms of this index",
        s"terms [.$${term}]"),
      snippetCompletion(
        "values",
        "Defines the values of this index",
        s"values [.$${value}]")
    )

  def RoleCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "privileges",
        "Grants privileges to the following resource",
        s"""|privileges $${resource} {
            |\t$${privilege}
            |}""".stripMargin
      ),
      snippetCompletion(
        "membership",
        "Grants membership to the following resource",
        s"membership $${resource}")
    )

  def ActionCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "read",
        "Grants read access to the given resource.",
        s"read$${0}"),
      snippetCompletion(
        "history_read",
        "Grants access to read historical versions of the given resource.",
        s"history_read$${0}"),
      snippetCompletion(
        "write",
        "Grants write access to the given resource.",
        s"write$${0}"),
      snippetCompletion(
        "create",
        "Grants access to create documents within the given resource.",
        s"create$${0}"),
      snippetCompletion(
        "create_with_id",
        "Grants access to create documents with a custom ID in the given resource.",
        s"create_with_id$${0}"),
      snippetCompletion(
        "delete",
        "Grants access to delete documents within this resource.",
        s"delete$${0}"),
      snippetCompletion(
        "call",
        "Grants access to call this resource. Only has an effect on functions.",
        s"call$${0}")
    )

  def AccessProviderCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "issuer",
        "Sets the issuer for this access provider",
        s"issuer \"$${issuer}\""),
      snippetCompletion(
        "jwks_uri",
        "Sets the JWKS URI for this access provider",
        s"jwks_uri \"$${jwks_uri}\""),
      snippetCompletion(
        "role",
        "Adds a role this access provider will grant",
        s"role $${role}")
    )

  def PredicateCompletions(implicit identSpan: Span) =
    Seq(
      snippetCompletion(
        "predicate",
        "Adds an FQL predicate to the given role",
        s"predicate ($${lambda})"))

  type CompletionResult = Seq[CompletionItem]

  protected def walkBlock(
    value: Option[FSL.Value],
    walkItem: FSL.Node => CompletionResult)(
    implicit cursor: Int,
    identSpan: Span): Option[CompletionResult] = {
    value match {
      case Some(FSL.Block(open, items, close))
          if cursor >= open.end && cursor <= close.start =>
        Some(walk(items, walkItem))
      case _ => None
    }
  }

  protected def walk(items: Seq[FSL.Node], walkItem: FSL.Node => CompletionResult)(
    implicit cursor: Int,
    identSpan: Span): CompletionResult = {
    val _ = identSpan

    val item = items
      .find(_.span.contains(cursor))
      .getOrElse(
        // This is effectively an "empty" item. It works surprisingly well, as the
        // cursor is within the `keyword`, so all the `walkItem` impls just return
        // keyword completions.
        FSL.Node(
          Seq.empty,
          Name("", Span(cursor, cursor, Src.Null)),
          None,
          None,
          Span(cursor, cursor, Src.Null)
        )
      )
    walkItem(item)
  }

  protected def walkTopLevel(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    val _ = (cursor, identSpan)
    if (item.annotations.exists(_.span.contains(cursor))) {
      walkAnnotations(item.annotations)
    } else if (item.keyword.span.contains(cursor)) {
      TopLevelCompletions
    } else if (item.name.exists(_.span.contains(cursor))) {
      Seq.empty
    } else {
      item match {
        case FSL.Node(_, Name("collection", _), _, value, _, _) =>
          val docShape = inferDocShape(item)
          val definedFields = inferFields(item)
          walkBlock(value, walkCollection(docShape, definedFields)).getOrElse(
            TopLevelCompletions)

        case FSL.Node(_, Name("function", _), _, _, _, _) =>
          walkFunction(item)

        case FSL.Node(_, Name("role", _), _, value, _, _) =>
          walkBlock(value, walkRole).getOrElse(TopLevelCompletions)

        case FSL.Node(_, Name("access provider", _), _, _, _, _) =>
          AccessProviderCompletions

        case _ => TopLevelCompletions
      }
    }
  }

  /** This runs through a collection item and builds the inferred doc type for that
    * collection.
    *
    * Note that this will ignore the cursor position, so it should only be called
    * when the `faunaRulezz` string isn't inserted into the collection definition.
    * This is safe to call if the cursor is within the body of a check constraint or
    * computed field.
    */
  private def inferDocShape(item: FSL.Node): TypeShape = {
    val docName = item.name.map(_.str).getOrElse("unknown")
    val fields = SeqMap.newBuilder[String, TypeScheme]
    // TODO: This should really use Typer.typeEnv to handle the recursive nature of
    // computed fields. For now we just type docs to `Any` within the computed field
    // lambda.
    val typer = Typer(globals, types)

    def addField(name: String, args: Seq[Option[Name]], body: Expr) = {
      val vctx = args.flatMap { nameOpt =>
        nameOpt.map { name =>
          name.str -> Type.Named(docName).typescheme
        }
      }.toMap
      val typer = Typer(
        globals,
        types ++ Map(
          docName -> TypeShape(
            self = Type.Named(docName).typescheme,
            alias = Some(Type.AnyRecord.typescheme),
            docType = TypeShape.DocType.Doc(docName)))
      )
      fields ++= typer.typeExpr(body, vctx).toOption.map { v =>
        name -> TypeScheme.Polymorphic(v, Type.Level.Zero)
      }
    }

    def addFSL(
      name: Name,
      ty: Option[FSL.ColonPartialType],
      value: Option[FSL.Value]) = {
      ty match {
        case Some(ty) =>
          val te = TypeExprParser.typeExpr(ty.ty)(EmptyEmitter)
          fields ++= typer.typeTExprType(te).toOption.map { ty =>
            name.str -> ty.typescheme
          }

        case None =>
          value match {
            case Some(FSL.Lit(Expr.Lambda(args, _, body, _))) =>
              addField(name.str, args, body)
            case Some(FSL.Lit(Expr.Tuple(Seq(Expr.Lambda(args, _, body, _)), _))) =>
              addField(name.str, args, body)

            case _ => ()
          }
      }
    }

    item.body match {
      case Some(FSL.Block(_, items, _)) =>
        items.foreach {
          case FSL.Node(_, FSL.Field.Keyword(_), Some(name), value, _, _) =>
            value match {
              case Some(FSL.Field(FSL.FieldKind.Compute, ty, value)) =>
                // Computed fields can infer their types from the value.
                addFSL(name, ty, value)

              case Some(FSL.Field(FSL.FieldKind.Defined, ty, _)) =>
                // Defined fields shouldn't infer types.
                addFSL(name, ty, None)

              case _ => ()
            }
          case _ => ()
        }
      case _ => ()
    }

    TypeShape(
      self = Type.Named(docName).typescheme,
      fields = fields.result(),
      docType = TypeShape.DocType.Doc(docName)
    )
  }

  private def inferFields(item: FSL.Node): SeqMap[String, Type] = {
    val fields = SeqMap.newBuilder[String, Type]
    // TODO: This should really use Typer.typeEnv handle the recursive nature of
    // computed fields. For now we just type docs to `Any` within the computed field
    // lambda.
    val typer = Typer(globals, types)

    def addFSL(name: Name, ty: Option[FSL.ColonPartialType]) = {
      ty match {
        case Some(ty) =>
          val te = TypeExprParser.typeExpr(ty.ty)(EmptyEmitter)
          fields ++= typer.typeTExprType(te).toOption.map { ty =>
            name.str -> ty
          }

        case None => ()
      }
    }

    item.body match {
      case Some(FSL.Block(_, items, _)) =>
        items.foreach {
          case FSL.Node(_, FSL.Field.Keyword(_), Some(name), value, _, _) =>
            value match {
              case Some(FSL.Field(FSL.FieldKind.Defined, ty, _)) =>
                addFSL(name, ty)

              case _ => ()
            }
          case _ => ()
        }
      case _ => ()
    }

    fields.result()
  }

  protected def walkCollection(
    docShape: TypeShape,
    definedFields: SeqMap[String, Type])(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    if (item.keyword.span.contains(cursor)) {
      CollectionCompletions
    } else if (item.name.exists(_.span.contains(cursor))) {
      Seq.empty
    } else {
      val docName = item.name.map(_.str).getOrElse("unknown")
      val docType = Type.Named(docName).typescheme
      val typer = Typer(globals, types ++ Map(docName -> docShape))

      item match {
        case FSL.Node(_, Name("index", _), _, _, _, _) =>
          val names = item.body match {
            case Some(FSL.Block(_, items, _)) => items.map(_.keyword.str)
            case _                            => Seq.empty
          }
          walkBlock(item.body, walkIndex(names)).getOrElse(CollectionCompletions)

        case FSL.Node(_, FSL.Field.Keyword(_), _, _, _, _) =>
          def completeComputedField(
            ty: Option[FSL.ColonPartialType],
            args: Seq[Option[Name]],
            body: Expr) = {
            val argTypes =
              args.flatMap { nameOpt =>
                nameOpt.map { name => name.str -> docType }
              }.toMap
            val expected =
              ty.flatMap(ty => {
                val te = TypeExprParser.typeExpr(ty.ty)(EmptyEmitter)
                typer.typeTExpr(te).toOption
              }) match {
                case Some(ts) => ts.raw.asInstanceOf[Type]
                case None     => Type.Any
              }

            walk(body, expected, argTypes, typer)
          }

          item.body match {
            case Some(
                  FSL.Field(
                    FSL.FieldKind.Compute,
                    ty,
                    Some(
                      FSL.Lit(Expr.Tuple(Seq(Expr.Lambda(args, _, body, _)), sp)))))
                if sp.contains(cursor) =>
              completeComputedField(ty, args, body)

            case Some(
                  FSL.Field(
                    FSL.FieldKind.Compute,
                    ty,
                    Some(FSL.Lit(Expr.Lambda(args, _, body, sp)))))
                if sp.contains(cursor) =>
              completeComputedField(ty, args, body)

            case _ => CollectionCompletions
          }

        case FSL.Node(_, Name("check", _), _, _, _, _) =>
          item.body match {
            case Some(FSL.Lit(Expr.Tuple(Seq(Expr.Lambda(args, _, body, _)), sp)))
                if sp.contains(cursor) =>
              val argTypes =
                args.flatMap { nameOpt =>
                  nameOpt.map { name => name.str -> docType }
                }.toMap

              walk(body, Type.Any, argTypes, typer)
            case _ => CollectionCompletions
          }

        case FSL.Node(_, Name("migrations", _), _, _, _, _) =>
          walkBlock(item.body, walkMigration(typer, definedFields))
            .getOrElse(CollectionCompletions)

        case _ => CollectionCompletions
      }
    }
  }

  protected def walkIndex(alreadyDefined: Seq[String])(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    val completions =
      IndexCompletions.filter(compl => !alreadyDefined.contains(compl.label))
    if (item.keyword.span.contains(cursor)) {
      completions
    } else {
      // TODO: Complete terms/values path based off of fields defined
      Seq.empty
    }
  }

  // TODO: Decide what to do about completions now that fields are paths.
  private def headField(path: Path): Option[Name] = path match {
    case Path(Nil, _)                                => None
    case Path(PathElem.Field(field, _) :: Nil, span) => Some(Name(field, span))
    case Path(_, _)                                  => None
  }

  protected def walkMigration(typer: Typer, definedFields: SeqMap[String, Type])(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    if (item.keyword.span.contains(cursor)) {
      MigrationCompletions
    } else {
      item match {
        // Backfill has an expression, so we match into that.
        case FSL.Node(_, Name("backfill", _), _, _, _, _) =>
          item.body match {
            case Some(FSL.Migration.Backfill(_, FSL.Lit(expr), _))
                if expr.span.contains(cursor) =>
              val expected = item.name match {
                case Some(n) =>
                  definedFields.get(n.str) match {
                    case Some(ts) => ts.asInstanceOf[Type]
                    case None     => Type.Any
                  }
                case None => Type.Any
              }

              walk(expr, expected, Map.empty, typer)

            // If that fails, we're probably at the <field> in
            // `backfill <field> = <expr>`, so we just complete fields.
            case _ => fieldCompletions(definedFields)
          }

        // These are nice and simple.
        case FSL.Node(
              _,
              Name("add" | "drop" | "move_conflicts" | "move_wildcard", _),
              _,
              _,
              _,
              _) =>
          fieldCompletions(definedFields)

        case FSL.Node(_, Name("split", _), _, _, _, _) =>
          item.body match {
            case Some(FSL.Migration.Split(_, to, _)) =>
              // Don't auto-complete fields that have already been split into, but
              // keep the completion for the field that's being typed.
              fieldCompletions(definedFields).filter { c =>
                to.forall { f =>
                  headField(f).forall { _.str.trim != c.label } || f.span.contains(
                    cursor)
                }
              }

            case _ => fieldCompletions(definedFields)
          }

        case FSL.Node(_, Name("move", _), _, _, _, _) =>
          item.body match {
            case Some(FSL.Migration.Move(from, to, _)) =>
              // Don't auto-complete the other field in the move, but keep the
              // completion for the field that's being typed.
              val fields = Seq(from, to)
              fieldCompletions(definedFields).filter { c =>
                fields.forall { f =>
                  headField(f).forall { s => s.str.trim != c.label } || f.span
                    .contains(cursor)
                }
              }

            case _ => fieldCompletions(definedFields)
          }

        case _ => MigrationCompletions
      }
    }
  }

  protected def fieldCompletions(fields: SeqMap[String, Type])(
    implicit identSpan: Span): CompletionResult = {
    fields.map { case (name, ty) =>
      CompletionItem(
        name,
        ty.display,
        identSpan,
        replaceText = name,
        newCursor = identSpan.start + name.length,
        kind = CompletionKind.Field,
        retrigger = false
      )
    }.toSeq
  }

  protected def walkRole(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    if (item.keyword.span.contains(cursor)) {
      RoleCompletions
    } else if (item.name.exists(_.span.contains(cursor))) {
      Seq.empty
    } else if (item.keyword.str == "privileges") {
      val names = item.body match {
        case Some(FSL.Block(_, items, _)) => items.map(_.keyword.str)
        case _                            => Seq.empty
      }
      walkBlock(item.body, walkAction(names)).getOrElse(RoleCompletions)
    } else if (item.keyword.str == "membership") {
      PredicateCompletions
    } else {
      RoleCompletions
    }
  }

  protected def walkAction(alreadyDefined: Seq[String])(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    val completions =
      ActionCompletions.filter(compl => !alreadyDefined.contains(compl.label))
    if (item.keyword.span.contains(cursor)) {
      completions
    } else {
      walkBlock(item.body, walkPredicate).getOrElse(completions)
    }
  }

  protected def walkPredicate(
    item: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    if (item.keyword.span.contains(cursor)) {
      PredicateCompletions
    } else {
      Seq.empty
    }
  }

  protected def walkAnnotations(annotations: Seq[FSL.Annotation])(
    implicit cursor: Int,
    identSpan: Span): CompletionResult = {
    val _ = (annotations, cursor, identSpan)
    TopLevelCompletions
  }

  protected def walkFunction(
    func: FSL.Node)(implicit cursor: Int, identSpan: Span): CompletionResult = {
    if (func.keyword.span.contains(cursor)) {
      TopLevelCompletions
    } else if (func.name.exists(_.span.contains(cursor))) {
      Seq.empty
    } else {
      func.body match {
        case Some(f: FSL.Function) =>
          if (cursor >= f.openParen.end && cursor <= f.closeParen.start) {
            walkArgs(f.args, f.variadic)
          } else if (
            f.ret.exists(ty => cursor >= ty.colon.end && cursor <= ty.ty.span.end)
          ) {
            walk(f.ret.get.ty)
          } else if (f.body.span.contains(cursor)) {
            val args = f.args ++ f.variadic.map(_.toArg)
            val typer = Typer(globals, types)
            val argTypes =
              args.map { case FSL.Arg(name, colonTy) =>
                name.str -> (colonTy.flatMap(ty =>
                  typer.typeTExpr(ty.ty).toOption) match {
                  case Some(ts) => ts
                  case None     => TypeScheme.Simple(Type.Any)
                })
              }.toMap
            val expected =
              f.ret.flatMap(ty => typer.typeTExpr(ty.ty).toOption) match {
                case Some(ts) => ts.raw.asInstanceOf[Type]
                case None     => Type.Any
              }

            walk(f.body, expected, argTypes, typer)
          } else {
            TopLevelCompletions
          }
        case _ => TopLevelCompletions
      }
    }

  }

  protected def walkArgs(args: Seq[FSL.Arg], variadic: Option[FSL.VarArg])(
    implicit cursor: Int,
    identSpan: Span): CompletionResult = {
    val allArgs = args ++ variadic.map(_.toArg)
    allArgs.findLast { arg =>
      cursor >= arg.name.span.start || arg.ty.exists(cursor >= _.ty.span.start)
    } match {
      case Some(FSL.Arg(name, _)) if name.span.contains(cursor) => Seq.empty
      case Some(FSL.Arg(_, Some(colonTy))) if cursor >= colonTy.colon.end =>
        walk(colonTy.ty)

      // cursor is elsewhere
      case _ => Seq.empty
    }
  }

  protected def walk(
    te: TypeExpr)(implicit cursor: Int, identSpan: Span): CompletionResult = {

    // Need new names so this new ContextWalk can capture these variables
    val globals0 = globals
    val types0 = types
    val typer0 = Typer(globals, types)
    typer0.recordingSpans = true
    typer0.typeTExpr(te)

    val ctx = new ContextWalk {
      val globals = globals0
      val types = types0
      val typer = typer0
    }

    ctx.walk(te)(cursor, MMap.empty).prioritizedCandidates(typer0, identSpan)
  }

  protected def walk(
    expr: Expr,
    expected: Type,
    vctx: Map[String, TypeScheme],
    typer: Typer)(implicit cursor: Int, identSpan: Span): CompletionResult = {

    // Need new names so this new ContextWalk can capture these variables
    val globals0 = globals
    val types0 = types
    val typer0 = typer
    typer0.recordingSpans = true
    typer0.typeExpr(expr, vctx)

    val ctx = new ContextWalk {
      val globals = globals0
      val types = types0
      val typer = typer0
    }

    ctx
      .walk(expr, expected)(cursor, vctx.view.mapValues(_.raw).to(MMap))
      .prioritizedCandidates(typer0, identSpan)
  }
}
