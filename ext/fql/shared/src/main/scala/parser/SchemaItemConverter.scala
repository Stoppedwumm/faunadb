package fql.parser

import fql.ast._
import fql.error.{ DiagnosticEmitter, Error, Hint, ParseError }
import fql.Result
import scala.collection.mutable.{ Map => MMap }

object SchemaItemConverter {
  def apply(items: Seq[FSL.Node]): Result[Seq[SchemaItem]] = {
    val c = new SchemaItemConverter
    val res = c(items)
    val errors = c.errors.result()
    if (errors.isEmpty) {
      Result.Ok(res)
    } else {
      new Result.Err(errors)
    }
  }
}

private final class SchemaItemConverter extends DiagnosticEmitter[ParseError] {
  import SchemaItem._

  val errors = List.newBuilder[Error]

  def emit(e: ParseError): Unit = errors += e
  def emit(msg: String, span: Span): Unit = errors += ParseError(msg, span)

  // Utilities

  def value(v: Option[FSL.Value], span: Span): Option[FSL.Value] = v match {
    case Some(v) => Some(v)
    case None =>
      emit("Missing required value", span)
      None
  }

  def blockOpt(v: Option[FSL.Value]): Option[FSL.Block] = v match {
    case Some(block: FSL.Block) => Some(block)
    case Some(other) =>
      emit("Expected a block", other.span)
      None
    case None => None
  }

  def name(name: Option[Name], span: Span): Option[Name] = name match {
    case Some(name) => Some(name)
    case None =>
      emit("Missing required name", span)
      None
  }

  // Special cases the "* identifier" for wildcard fields.
  def starIdentifier(name: Option[Name], span: Span): Option[Name] = name match {
    case Some(Name("*", _)) => name
    case _                  => identifier(name, span)
  }

  def identifier(name: Option[Name], span: Span): Option[Name] = name match {
    case Some(name) if Tokens.isValidIdent(name.str) =>
      Some(name)
    case Some(name) =>
      emit(s"Invalid identifier `${name.str}`", name.span)
      None
    case None =>
      emit("Missing required name", span)
      None
  }

  def unreservedStarIdentifier(name: Option[Name], span: Span): Option[Name] =
    starIdentifier(name, span) flatMap {
      case Name(n, _) if Tokens.SpecialFieldNames.contains(n) =>
        emit(s"Invalid reserved name `$n`", span)
        None
      case _ => name
    }

  def allowedAnns(anns: Seq[FSL.Annotation], kind: Annotation.Kind*) = {
    val set = kind.view.map { _.keyword }.toSet
    val invalid = anns collect { case ann if !set(ann.keyword.str) => ann.span }
    invalid foreach { emit("Annotation not supported here", _) }
  }

  def annotation(anns: Seq[FSL.Annotation], kind: Annotation.Kind) = {
    val collected = anns collect {
      case ann if ann.keyword.str == kind.keyword =>
        Annotation(kind, Config.Id(ann.value), ann.span)
    }

    if (collected.sizeIs > 1) {
      val first = collected.head
      collected.tail foreach { ann =>
        emit(
          ParseError(
            s"Duplicate annotation `${ann.kind.keyword}`",
            ann.span,
            hints = Seq(Hint("Originally defined here", first.span))
          ))
      }
    }

    collected.headOption
  }

  def noAnnotations(annotations: Seq[FSL.Annotation]) =
    if (annotations.nonEmpty) {
      emit("Annotations are not allowed here", annotations.toSeq.head.span)
    }

  def noName(name: Option[Name]) = name match {
    case Some(name) =>
      emit("Name is not allowed here", name.span)
    case None =>
  }

  def block[T](value: FSL.Value, conv: Seq[FSL.Node] => T): Option[T] = {
    value match {
      case FSL.Block(_, items, _) => Some(conv(items))
      case other =>
        emit("Expected a block", other.span)
        None
    }
  }

  def blockWithSpans[T](
    value: FSL.Value,
    conv: Seq[FSL.Node] => Seq[T]): Option[(Span, Seq[T], Span)] = {
    value match {
      case FSL.Block(open, items, close) =>
        Some((open, conv(items), close))
      case other =>
        emit("Expected a block", other.span)
        None
    }
  }

  // Converters

  def apply(items: Seq[FSL.Node]): Seq[SchemaItem] = {
    // collections and functions share a namespace, others do not.
    val collFunctionDupe = new DupeList(this, "collection or function")
    val roleDupe = new DupeList(this, "role")
    val accessProviderDupe = new DupeList(this, "access provider")

    items flatMap {
      case FSL.Node(anns, Kind.Collection(kw), name, block, span, docComment) =>
        for {
          name <- this.name(name, kw)
          _ = this.allowedAnns(anns, Annotation.Kind.Alias)
          alias = this.annotation(anns, Annotation.Kind.Alias)
          _ = {
            collFunctionDupe.check(name)
            alias.foreach { ann =>
              collFunctionDupe.check(ann.configValue)
            }
          }
          b <- this.value(block, span)
          (
            fields,
            migrations,
            indexes,
            uniques,
            checks,
            histDays,
            ttlDays,
            docTTLs) <- this
            .block(b, this.collFieldsAndMembers)
        } yield {
          Collection(
            name,
            alias,
            fields,
            migrations,
            indexes,
            uniques,
            checks,
            histDays,
            ttlDays,
            docTTLs,
            span,
            docComment
          )
        }

      case FSL.Node(anns, Kind.Function(kw), name, block, span, docComment) =>
        for {
          name <- this.name(name, kw)
          _ = this.allowedAnns(anns, Annotation.Kind.Alias, Annotation.Kind.Role)
          alias = this.annotation(anns, Annotation.Kind.Alias)
          _ = {
            (collFunctionDupe.contains(name), alias) match {
              // Function names may conflict with collections if an alias is
              // present.
              case (true, Some(ann)) if items.exists {
                    case FSL.Node(_, Kind.Collection(_), n, _, _, _) =>
                      n.exists(_.str == name.str)
                    case _ => false
                  } =>
                collFunctionDupe.check(ann.configValue)

              // Otherwise, the function name and alias must be unique.
              case _ =>
                collFunctionDupe.check(name)
                alias.foreach { ann =>
                  collFunctionDupe.check(ann.configValue)
                }
            }
          }
          b    <- this.value(block, span)
          func <- this.function(b, kw, anns, alias, name, docComment)
        } yield func

      case FSL.Node(anns, Kind.Role(kw), name, block, span, docComment) =>
        this.noAnnotations(anns)
        for {
          name <- this.name(name, kw)
          _ = roleDupe.check(name)
          b                         <- this.value(block, span)
          (privileges, memberships) <- this.block(b, this.roleMembers)
        } yield Role(name, privileges, memberships, span, docComment)

      case FSL.Node(anns, Kind.AccessProvider(kw), name, block, span, docComment) =>
        this.noAnnotations(anns)
        for {
          name <- this.name(name, kw)
          _ = accessProviderDupe.check(name)
          b <- this.value(block, span)
          (issuer, jwksURI, roles) <- b match {
            case FSL.Block(open, items, close) =>
              Some(this.accessProviderMembers(items, open.copy(end = close.end)))
            case other =>
              emit("Expected a block", other.span)
              None
          }
        } yield AccessProvider(name, issuer, jwksURI, roles, span, docComment)

      case FSL.Node(_, Name(name, sp), _, _, _, _) =>
        emit(s"Invalid schema item `$name`", sp)
        None
    }
  }

  def collFieldsAndMembers(items: Seq[FSL.Node]): (
    Seq[Field],
    Option[Member.Typed[MigrationBlock]],
    Seq[Collection.Index],
    Seq[Collection.Unique],
    Seq[Collection.CheckConstraint],
    Collection.HistoryDays,
    Option[Collection.TTLDays],
    Option[Collection.DocumentTTLs]
  ) = {
    val indexDupe = new DupeList(this, "index name")
    val migrationsDupe = new DupeItem(this, "migration block")
    val checkDupe = new DupeList(this, "check constraint name")
    val histDupe = new DupeItem(this, "history days")
    val ttlDupe = new DupeItem(this, "ttl days")
    val docTTLDupe = new DupeItem(this, "document TTLs")
    val fieldDupe = new DupeList(this, "field")
    val uniqueDupe = new DupeUnique(this, "unique constraint")

    val fields = Seq.newBuilder[Field]
    var migrations = Option.empty[Member.Typed[MigrationBlock]]
    val indexes = Seq.newBuilder[Collection.Index]
    val uniques = Seq.newBuilder[Collection.Unique]
    val checks = Seq.newBuilder[Collection.CheckConstraint]
    var histDays = Option.empty[Collection.HistoryDays]
    var ttlDays = Option.empty[Collection.TTLDays]
    var docTTLs = Option.empty[Collection.DocumentTTLs]

    items foreach {
      case FSL.Node(anns, Member.Kind.Index(kw), name, block, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.identifier(name, kw)
          _ = indexDupe.check(name)
          b               <- this.value(block, span)
          (terms, values) <- this.block(b, this.indexMembers)
        } yield {
          indexes += Member.Named(
            name,
            Member(
              Member.Kind.Index,
              Collection.IndexConfig(terms, values, b.span),
              span
            ))
        }

      case FSL.Node(anns, Member.Kind.Unique(_), name, value, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        for {
          value <- this.value(value, span)
          terms <- this.nonEmptyIndexTerms(value)
          _ = uniqueDupe.check(terms)
        } yield {
          uniques += Member.Repeated(
            Member(
              Member.Kind.Unique,
              terms,
              span
            ))
        }

      case FSL.Node(anns, FSL.Field.Keyword(kwSpan), name, value, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.unreservedStarIdentifier(name, span)
          _ = fieldDupe.check(name)
          value <- this.value(value, span)
          field <- this.field(kwSpan, name, value, span)
        } yield {
          fields += field
        }

      case FSL.Node(anns, Member.Kind.Migrations(_), name, lit, span, _) =>
        this.noAnnotations(anns)
        migrationsDupe.check(span)
        for {
          value <- this.value(lit, span)
          _ = this.noName(name)
          ms <- this.migrationBlock(value, span)
        } yield {
          migrations = Some(ms)
        }

      case FSL.Node(anns, Member.Kind.Check(kw), name, value, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.identifier(name, kw)
          _ = checkDupe.check(name)
          v    <- this.value(value, span)
          pred <- this.checkFunction(v)
        } yield {
          checks += Member.Named(name, Member(Member.Kind.Check, pred, span))
        }

      case FSL.Node(anns, Member.Kind.HistoryDays(_), name, lit, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        histDupe.check(span)
        for {
          value <- this.value(lit, span)
          days  <- this.long(value)
        } yield {
          histDays = Some(
            Member.Default.Set(
              Member(
                Member.Kind.HistoryDays,
                days,
                span
              ),
              isDefault =
                days.value == SchemaItem.Collection.DefaultHistoryDays.configValue
            ))
        }

      case FSL.Node(anns, Member.Kind.TTLDays(_), name, lit, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        ttlDupe.check(span)
        for {
          value <- this.value(lit, span)
          days  <- this.long(value)
        } yield {
          ttlDays = Some(
            Member(
              Member.Kind.TTLDays,
              days,
              span
            ))
        }

      case FSL.Node(anns, Member.Kind.DocumentTTLs(_), name, lit, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        docTTLDupe.check(span)
        for {
          value   <- this.value(lit, span)
          enabled <- this.bool(value)
        } yield {
          docTTLs = Some(Member(Member.Kind.DocumentTTLs, enabled, span))
        }

      case FSL.Node(_, Name(name, sp), _, _, _, _) =>
        emit(s"Invalid field or member `$name`", sp)
        None
    }

    (
      fields.result(),
      migrations,
      indexes.result(),
      uniques.result(),
      checks.result(),
      histDays.getOrElse(SchemaItem.Collection.DefaultHistoryDays),
      ttlDays,
      docTTLs)
  }

  def indexMembers(items: Seq[FSL.Node]): (
    Option[Collection.Index.Terms],
    Option[Collection.Index.Values]
  ) = {
    val termsDupe = new DupeItem(this, "terms definition")
    val valuesDupe = new DupeItem(this, "values definition")

    var terms = Option.empty[Collection.Index.Terms]
    var values = Option.empty[Collection.Index.Values]

    items foreach {
      case FSL.Node(anns, Member.Kind.Terms(_), name, paths, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        termsDupe.check(span)
        for {
          value  <- this.value(paths, span)
          terms0 <- this.indexTerms(value)
        } {
          terms = Some(Member(Member.Kind.Terms, terms0, span))
        }

      case FSL.Node(anns, Member.Kind.Values(_), name, paths, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        valuesDupe.check(span)
        for {
          value   <- this.value(paths, span)
          values0 <- this.indexValues(value)
        } {
          values = Some(Member(Member.Kind.Values, values0, span))
        }

      case FSL.Node(_, Name(name, sp), _, _, _, _) =>
        emit(s"Invalid member `$name`", sp)
    }

    (terms, values)
  }

  def roleMembers(items: Seq[FSL.Node]): (
    Seq[Role.Privilege],
    Seq[Role.Membership]
  ) = {
    val privilegesDupe = new DupeList(this, "privileges")
    val membershipDupe = new DupeList(this, "membership")

    val privileges = Seq.newBuilder[Role.Privilege]
    val memberships = Seq.newBuilder[Role.Membership]

    items foreach {
      case FSL.Node(anns, Member.Kind.Privileges(kw), name, block, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.name(name, kw)
          _ = privilegesDupe.check(name)
          b                      <- this.value(block, span)
          (open, actions, close) <- this.blockWithSpans(b, this.actions)
        } yield {
          privileges += Member.Named(
            name,
            Member(
              Member.Kind.Privileges,
              Role.PrivilegeConfig(actions, open.to(close)),
              span
            ))
        }

      case FSL.Node(anns, Member.Kind.Membership(kw), name, block, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.name(name, kw)
          _ = membershipDupe.check(name)
          pred = this.blockOpt(block) flatMap { this.predicateBlock(_) }
        } yield {
          memberships += Member.Named(
            name,
            Member(
              Member.Kind.Membership,
              Config.Opt(pred),
              span
            ))
        }

      case FSL.Node(_, Name(name, sp), _, _, _, _) =>
        emit(s"Invalid member `$name`", sp)
        None
    }

    (privileges.result(), memberships.result())
  }

  def accessProviderMembers(items: Seq[FSL.Node], span: Span): (
    AccessProvider.Issuer,
    AccessProvider.JWKSURI,
    Seq[AccessProvider.Role]
  ) = {
    val issuerDupe = new DupeItem(this, "issuer")
    val jwksUriDupe = new DupeItem(this, "jwks uri")
    val rolesDupe = new DupeList(this, "role")

    // These will get set, or an error will be emitted by `assertSeen` below.
    var issuer: AccessProvider.Issuer = null
    var jwksUri: AccessProvider.JWKSURI = null
    val roles = Seq.newBuilder[AccessProvider.Role]

    items foreach {
      case FSL.Node(anns, Member.Kind.Issuer(kw), name, value, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        issuerDupe.check(kw)
        for {
          v   <- this.value(value, span)
          str <- this.str(v)
        } {
          issuer = Member(Member.Kind.Issuer, str, span)
        }

      case FSL.Node(anns, Member.Kind.JWKSURI(kw), name, value, span, _) =>
        this.noAnnotations(anns)
        this.noName(name)
        jwksUriDupe.check(kw)
        for {
          v   <- this.value(value, span)
          str <- this.str(v)
        } {
          jwksUri = Member(Member.Kind.JWKSURI, str, span)
        }

      case FSL.Node(anns, Member.Kind.Role(_), name, block, span, _) =>
        this.noAnnotations(anns)
        for {
          name <- this.name(name, span)
          _ = rolesDupe.check(name)
          pred = this.blockOpt(block) flatMap { this.predicateBlock(_) }
        } {
          roles += Member.Named(
            name,
            Member(
              Member.Kind.Role,
              Config.Opt(pred),
              span
            ))
        }

      case FSL.Node(_, Name(name, sp), _, _, _, _) =>
        emit(s"Invalid member `$name`", sp)
    }

    issuerDupe.assertSeen(span)
    jwksUriDupe.assertSeen(span)

    (issuer, jwksUri, roles.result())
  }

  def actions(items: Seq[FSL.Node]): Seq[Role.Action] = {
    val actionsDupe = new DupeList(this, "action")

    items flatMap { case FSL.Node(anns, actionName, name, block, span, _) =>
      this.noAnnotations(anns)
      this.noName(name)
      for {
        kind <- this.action(actionName)
        _ = actionsDupe.check(actionName)
        pred = this.blockOpt(block) flatMap { this.predicateBlock(_) }
      } yield {
        Member(kind, Config.Opt(pred), span)
      }
    }
  }

  def field(kwSpan: Span, name: Name, value: FSL.Value, span: Span): Option[Field] =
    value match {
      case FSL.Field(FSL.FieldKind.Compute, ct, v) =>
        val ty = ct.map { _.ty }
        v match {
          case Some(FSL.Lit(lambda: Expr.Lambda)) =>
            Some(
              Field
                .Computed(
                  kwSpan,
                  name,
                  ty.map(TypeExprParser.typeExpr(_)(this)),
                  Config.Lambda(lambda, lambda.span),
                  span))

          case Some(FSL.Lit(Expr.Tuple(Seq(lambda: Expr.Lambda), _))) =>
            Some(
              Field
                .Computed(
                  kwSpan,
                  name,
                  ty.map(TypeExprParser.typeExpr(_)(this)),
                  Config.Lambda(lambda, lambda.span),
                  span))

          case Some(FSL.Lit(other)) =>
            emit("Expected lambda", other.span)
            None
          case Some(other) =>
            emit("Expected an expression literal", other.span)
            None
          case None =>
            emit("Computed fields require a value", kwSpan)
            None
        }

      case FSL.Field(FSL.FieldKind.Defined, ct, v) =>
        (ct.map { _.ty }, v) match {
          case (None, _) =>
            emit("Expected a type signature", span)
            None
          case (Some(ty), Some(FSL.Lit(expr))) =>
            Some(
              Field.Defined(
                name,
                TypeExprParser.schemaTypeExpr(
                  ty,
                  reason = Some(NoDefaultReason.ParentHasDefault(expr)))(this),
                Some(Config.Expression(expr)),
                span))
          case (Some(ty), None) =>
            Some(
              Field
                .Defined(
                  name,
                  TypeExprParser.schemaTypeExpr(ty, reason = None)(this),
                  None,
                  span))
          case (_, Some(other)) =>
            emit("Expected an expression literal", other.span)
            None
        }

      case FSL.Field(FSL.FieldKind.Wildcard, Some(ct), None) =>
        Some(Field.Wildcard(TypeExprParser.typeExpr(ct.ty)(this), span))

      case FSL.Field(FSL.FieldKind.Wildcard, _, Some(v)) =>
        emit("No default allowed for wildcard field", v.span)
        None

      case other =>
        emit("Expected a field definition", other.span)
        None
    }

  private def checkPath(path: Path): Unit = path match {
    case Path(Nil, span) => emit("Field path cannot be empty", span)
    case Path(elems, _) =>
      elems.foreach {
        case PathElem.Field(_, _) => ()
        case PathElem.Index(_, span) =>
          emit("Cannot index into an item in a migration path", span)
      }
  }

  def migrationBlock(
    value: FSL.Value,
    span: Span): Option[Member.Typed[MigrationBlock]] =
    value match {
      case FSL.Block(_, items, _) =>
        val migrations = items.flatMap {
          case FSL.Node(anns, _, name, Some(m: FSL.Migration), _, _) =>
            this.noAnnotations(anns)
            this.noName(name)
            m match {
              case FSL.Migration.Backfill(path, value, span) =>
                checkPath(path)
                Some(MigrationItem.Backfill(path, value, span))
              case FSL.Migration.Drop(path, span) =>
                checkPath(path)
                Some(MigrationItem.Drop(path, span))
              case FSL.Migration.Split(outOf, into, span) =>
                checkPath(outOf)
                into.foreach(checkPath)
                if (into.size < 2) {
                  emit("Expected at least two fields to split into", span)
                  None
                } else if (into.toSet.size < into.size) {
                  emit(
                    "Expected list of fields with no duplicates",
                    into.head.span.to(into.last.span))
                  None
                } else {
                  Some(MigrationItem.Split(outOf, into, span))
                }
              case FSL.Migration.Move(from, to, span) =>
                checkPath(from)
                checkPath(to)
                Some(MigrationItem.Move(from, to, span))
              case FSL.Migration.Add(path, span) =>
                checkPath(path)
                Some(MigrationItem.Add(path, span))
              case FSL.Migration.MoveWildcardConflicts(path, span) =>
                checkPath(path)
                Some(MigrationItem.MoveWildcardConflicts(path, span))
              case FSL.Migration.MoveWildcard(path, span) =>
                checkPath(path)
                Some(MigrationItem.MoveWildcard(path, span))
              case FSL.Migration.AddWildcard(span) =>
                Some(MigrationItem.AddWildcard(span))
            }

          case FSL.Node(_, _, _, Some(other), _, _) =>
            emit("Only migrations are allowed in migration blocks", other.span)
            None

          case other =>
            emit("Expected a migration", other.span)
            None
        }

        if (items.isEmpty) {
          emit("Empty migration blocks are not allowed", span)
          None
        } else {
          Some(
            Member
              .Typed(Member.Kind.Migrations, MigrationBlock(migrations, span), span))
        }

      case other =>
        emit("Expected a migration block", other.span)
        None
    }

  def predicateBlock(block: FSL.Block): Option[Config.Predicate] = {
    if (block.items.length == 1) {
      block.items.head match {
        case FSL.Node(annotations, Name("predicate", _), n, value, sp, _) =>
          this.noAnnotations(annotations)
          this.noName(n)
          for {
            v <- this.value(value, sp)
            lambda <- v match {
              case FSL.Lit(Expr.Tuple(Seq(lambda: Expr.Lambda), _)) =>
                Some(lambda)
              case FSL.Lit(other) =>
                emit(ParseError("Expected lambda predicate", other.span))
                None
              case other =>
                emit(s"Expected an expression literal", other.span)
                None
            }
          } yield {
            Config.Predicate(lambda, block.span)
          }
        case item =>
          emit("Expecting a predicate", item.span)
          None
      }
    } else {
      emit("Expected a single predicate member", block.span)
      None
    }
  }

  def checkFunction(value: FSL.Value): Option[Config.CheckPredicate] = value match {
    case FSL.Lit(Expr.Tuple(Seq(lambda: Expr.Lambda), _)) =>
      Some(Config.CheckPredicate(lambda, value.span))
    case FSL.Lit(lambda: Expr.Lambda) =>
      emit(
        ParseError(
          "Expected lambda check predicate",
          lambda.span,
          Some("Check constraints require parenthesis around the lambda")
        ))
      None
    case FSL.Lit(other) =>
      emit(ParseError("Expected lambda check predicate", other.span))
      None
    case other =>
      emit(s"Expected an expression literal", other.span)
      None
  }

  def action(action: Name): Option[Member.Kind] =
    action.str match {
      case "create"            => Some(Member.Kind.Create)
      case "create_with_id"    => Some(Member.Kind.CreateWithId)
      case "delete"            => Some(Member.Kind.Delete)
      case "read"              => Some(Member.Kind.Read)
      case "write"             => Some(Member.Kind.Write)
      case "history_read"      => Some(Member.Kind.HistoryRead)
      case "history_write"     => Some(Member.Kind.HistoryWrite)
      case "unrestricted_read" => Some(Member.Kind.UnrestrictedRead)
      case "call"              => Some(Member.Kind.Call)
      case _ =>
        emit(s"Invalid action `${action.str}`", action.span)
        None
    }

  def long(v: FSL.Value): Option[Config.Long] =
    v match {
      case FSL.Lit(Expr.Lit(Literal.Int(v), span)) =>
        Some(Config.Long(v.toLong, span))
      case other =>
        emit(s"Expected integer literal", other.span)
        None
    }

  def str(v: FSL.Value): Option[Config.Str] =
    v match {
      case FSL.Lit(Expr.Lit(Literal.Str(v), span)) => Some(Config.Str(v, span))
      case FSL.Lit(Expr.StrTemplate(Seq(), span))  => Some(Config.Str("", span))
      case FSL.Lit(Expr.StrTemplate(Seq(Left(v)), span)) => Some(Config.Str(v, span))
      case FSL.Lit(Expr.StrTemplate(items, _)) =>
        items.foreach {
          case Left(_) =>
          case Right(expr) =>
            emit("Cannot interpolate values in a string literal", expr.span)
        }
        None
      case other =>
        emit(s"Expected string literal", other.span)
        None
    }

  def bool(v: FSL.Value): Option[Config.Bool] =
    v match {
      case FSL.Lit(Expr.Lit(Literal.True, span))  => Some(Config.Bool(true, span))
      case FSL.Lit(Expr.Lit(Literal.False, span)) => Some(Config.Bool(false, span))
      case other =>
        emit(s"Expected boolean literal", other.span)
        None
    }

  def indexTerms(v: FSL.Value): Option[Config.SeqT[Config.IndexTerm]] =
    v match {
      case FSL.Paths(leftBracket, paths, rightBracket) =>
        val pathsCfg = paths map { path =>
          val ((terms, errs), mva) = SchemaItemParser.termsFieldPath(path)
          errs foreach { emit(_) }
          Config.IndexTerm(terms, mva, path.span)
        }
        Option.when(pathsCfg.nonEmpty) {
          Config.Seq(pathsCfg, leftBracket.to(rightBracket))
        }
      case other =>
        emit(s"Expected an array of terms", other.span)
        None
    }

  def nonEmptyIndexTerms(v: FSL.Value): Option[Config.SeqT[Config.IndexTerm]] =
    indexTerms(v) match {
      case v if v.exists(_.configs.nonEmpty) => v
      case _ =>
        emit(s"Expected non-empty terms for a unique constraint", v.span)
        None
    }

  def indexValues(v: FSL.Value): Option[Config.SeqT[Config.IndexValue]] =
    v match {
      case FSL.Paths(leftBracket, paths, rightBracket) =>
        val pathsCfg = paths map { path =>
          val ((terms, errs), asc, mva) = SchemaItemParser.valuesFieldPath(path)
          errs foreach { emit(_) }
          Config.IndexValue(terms, mva, asc, path.span)
        }
        Option.when(pathsCfg.nonEmpty) {
          Config.Seq(pathsCfg, leftBracket.to(rightBracket))
        }
      case other =>
        emit(s"Expected an array of values", other.span)
        None
    }

  def function(
    v: FSL.Value,
    kwSpan: Span,
    anns: Seq[FSL.Annotation],
    alias: Option[SchemaItem.Function.Alias],
    name: Name,
    docComment: Option[Span]): Option[SchemaItem.Function] =
    v match {
      case FSL.Function(openParen, args, varArgs, closeParen, ret, body) =>
        val startSpan =
          docComment
            .orElse(anns.headOption map { _.span })
            .getOrElse(kwSpan)

        // Assert that all or no arguments have a type annotation
        if (args.nonEmpty || varArgs.nonEmpty) {
          val firstArg = (args ++ varArgs.map(_.toArg)).head
          val expectingTypes = firstArg.ty.isDefined

          (args ++ varArgs.map(_.toArg)).foreach { arg =>
            if (expectingTypes && arg.ty.isEmpty) {
              emit(
                ParseError(
                  s"All or no arguments must have a type",
                  arg.name.span,
                  hints = Seq(
                    Hint(
                      "Expecting types due to this type annotation",
                      firstArg.ty.get.ty.span))))
            } else if (!expectingTypes && arg.ty.isDefined) {
              emit(
                ParseError(
                  s"All or no arguments must have a type",
                  arg.name.span,
                  hints = Seq(
                    Hint(
                      "Expecting no types due to this argument not having a type",
                      firstArg.name.span))
                ))
            }
          }

          if (expectingTypes && ret.isEmpty) {
            emit(
              ParseError(
                s"Return type is required when arguments have a type",
                closeParen,
                hints = Seq(
                  Hint(
                    "Expecting types due to this type annotation",
                    firstArg.ty.get.ty.span))
              ))
          } else if (!expectingTypes && ret.isDefined) {
            emit(
              ParseError(
                s"Return type is not allowed when arguments do not have a type",
                ret.get.ty.span,
                hints = Seq(
                  Hint(
                    "Expecting no types due to this argument not having a type",
                    firstArg.name.span))
              ))
          }
        }

        val role = this.annotation(anns, Annotation.Kind.Role)

        val argsSeq =
          args.view
            .map { a => Function.Arg(a.name, a.ty map { _.ty }, variadic = false) }
            .concat(varArgs map { a =>
              Function.Arg(a.name, a.ty map { _.ty }, variadic = true)
            })
            .toSeq

        val sigSpan = openParen.to(ret.fold(closeParen) { _.span })
        val sig = Function.Sig(argsSeq, ret map { _.ty }, sigSpan)

        Some(
          SchemaItem.Function(
            name,
            alias,
            role,
            sig,
            body,
            startSpan.to(body.span),
            docComment
          ))
      case other =>
        emit(s"Expected a function definition", other.span)
        None
    }

}

private final class DupeList(val convert: SchemaItemConverter, val msg: String) {
  var seen = MMap.empty[String, Span]

  def contains(name: Name) = seen.contains(name.str)

  def check(name: Name) = seen.get(name.str) match {
    case Some(seen) =>
      convert.emit(
        ParseError(
          s"Duplicate $msg `${name.str}`",
          name.span,
          hints = Seq(Hint("Originally defined here", seen))))
    case None => seen.put(name.str, name.span)
  }
}

private final class DupeUnique(val convert: SchemaItemConverter, val msg: String) {
  var seen = MMap.empty[Set[Config.IndexTerm], Span]

  def check(terms: Config.SeqT[Config.IndexTerm]) = {
    val key = terms.configs
      .map(p => p.copy(path = p.path.withNullSpan, span = Span.Null))
      .toSet

    seen.get(key) match {
      case Some(seen) =>
        convert.emit(
          ParseError(
            s"Duplicate $msg.",
            terms.span,
            hints =
              Seq(Hint("This unique constraint covers the same fields.", seen))))
      case None => {
        seen.put(key, terms.span)
      }
    }
  }
}

private final class DupeItem(val convert: SchemaItemConverter, val msg: String) {
  var seen: Option[Span] = None

  def check(sp: Span) = seen match {
    case Some(seen) =>
      convert.emit(
        ParseError(
          s"Duplicate $msg",
          sp,
          hints = Seq(Hint("Originally defined here", seen))))
    case None => seen = Some(sp)
  }

  def assertSeen(span: Span) = if (seen.isEmpty) {
    convert.emit(s"Missing $msg", span)
  }
}
