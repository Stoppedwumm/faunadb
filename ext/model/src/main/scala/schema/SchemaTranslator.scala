package fauna.model.schema

import fauna.atoms._
import fauna.lang.syntax._
import fauna.model._
import fauna.model.schema.index.CollectionIndexManager
import fauna.repo.query.Query
import fauna.storage.doc.Data
import fauna.storage.ir._
import fql.{ Result => FQLResult }
import fql.ast._
import fql.parser.Parser
import fql.Result.Ok
import scala.concurrent.duration.Duration

object SchemaTranslator {

  def translateSchema(scope: ScopeID): Query[Seq[SchemaItem]] =
    for {
      colls <- translateCollections(scope)
      fns   <- translateFunctions(scope, colls)
      roles <- translateRoles(scope)
      aps   <- translateAPs(scope)
    } yield {
      val b = Seq.newBuilder[SchemaItem]
      b ++= colls
      b ++= fns
      b ++= roles
      b ++= aps
      b.result()
    }

  private def parseLambdaOrThrow(src: String) =
    Parser.lambdaExpr(src, Src.Null) match {
      case Ok(lambda) => lambda
      case _ =>
        throw new IllegalStateException("failed to parse stored function body")
    }

  private def parseSignatureOrThrow(src: String) =
    Parser.typeExpr(src, Src.Null) match {
      case Ok(te) => te
      case _ =>
        throw new IllegalStateException("failed to parse stored function body")
    }

  private def parseExprOrThrow(src: String) =
    Parser.expr(src, Src.Null) match {
      case Ok(expr) => expr
      case _ =>
        throw new IllegalStateException("failed to parse stored expr")
    }

  def translateCollections(scope: ScopeID) =
    CollectionID
      .getAllUserDefined(scope)
      .flattenT
      .flatMap { ids =>
        ids
          .map { id =>
            SchemaCollection.Collection(scope).getVersionLiveNoTTL(id).mapT { live =>
              translateCollection(scope, live.data)
            }
          }
          .sequence
          .map { _.flatten }
      }

  def translateCollection(scope: ScopeID, data: Data) = {
    val collName = SchemaNames.findName(scope, data)

    val indexes0 = CollectionIndexManager.UserIndexDefinition.fromData(data)
    val indexes = indexes0.values.collect {
      case idx: CollectionIndexManager.UserIndexDefinition =>
        val terms0 = idx.terms.map { _.toConfig }
        val terms =
          Member(Member.Kind.Terms, Config.Seq(terms0, Span.Null), Span.Null)

        val values0 = idx.values.map { _.toConfig }
        val values =
          Member(Member.Kind.Values, Config.Seq(values0, Span.Null), Span.Null)

        val config = SchemaItem.Collection.IndexConfig(
          Option.when(terms0.nonEmpty)(terms),
          Option.when(values0.nonEmpty)(values),
          Span.Null)

        Member.Named(
          Name(idx.name, Span.Null),
          Member(Member.Kind.Index, config, Span.Null))
    }.toSeq

    val uniqueConstraints = CollectionIndexManager.UniqueConstraint.fromData(data)
    val uniques = uniqueConstraints collect {
      case idx: CollectionIndexManager.UniqueConstraint =>
        val terms0 = idx.fields.map { _.toConfig }

        Member.Repeated(
          Member(Member.Kind.Unique, Config.Seq(terms0, Span.Null), Span.Null))
    }

    val definedFields = DefinedField.fromData(collName, data)
    val pfs = definedFields.view.map { case (name, f) =>
      Field.Defined(
        Name(name, Span.Null),
        f.expectedTypeExpr,
        f.default.map { str =>
          Config.Expression(Parser.expr(str, Src.Inline("foo", str)) match {
            case fql.Result.Ok(e) => e
            case fql.Result.Err(e) =>
              throw new IllegalStateException(s"failed to parse default value $e")
          })
        },
        Span.Null
      )
    }

    val computedFields = ComputedField.fromData(collName, data)
    val cfs = computedFields.view.map { case (name, cf) =>
      // TODO: Parse and extract field type from validated signature.
      Field.Computed(
        Span.Null,
        Name(name, Span.Null),
        cf.signature.map(parseSignatureOrThrow),
        Config.Lambda(parseLambdaOrThrow(cf.body), Span.Null),
        Span.Null)
    }

    val wildcard = Wildcard.fromData(collName, data)
    val wc = wildcard map { wc =>
      Field.Wildcard(wc.expectedTypeExpr, Span.Null)
    }

    val fields = (pfs ++ cfs ++ wc).toSeq

    val migrations = translateMigrationBlock(data)

    val checkConstraints = CheckConstraint.fromData(data)
    val checks = checkConstraints map { case CheckConstraint(name, body) =>
      Member.Named(
        Name(name, Span.Null),
        Member(
          Member.Kind.Check,
          Config.CheckPredicate(parseLambdaOrThrow(body), Span.Null),
          Span.Null))
    }

    val historyDuration =
      data(Collection.RetainDaysField)
        .fold(Duration.Inf: Duration)(_.saturatedDays)

    val days = if (historyDuration.isFinite) {
      historyDuration.toDays
    } else {
      // Should hopefully be long enough.
      Long.MaxValue
    }
    val hd =
      if (days == SchemaItem.Collection.DefaultHistoryDays.configValue) {
        SchemaItem.Collection.DefaultHistoryDays
      } else {
        Member.Default.Set(
          Member(
            Member.Kind.HistoryDays,
            Config.Long(days, Span.Null),
            Span.Null
          ),
          isDefault = false
        )
      }

    val ttlDuration =
      data(Collection.TTLField).fold(Duration.Inf: Duration)(_.saturatedDays)
    val ttld = Option.when(ttlDuration.isFinite) {
      Member(
        Member.Kind.TTLDays,
        Config.Long(ttlDuration.toDays, Span.Null),
        Span.Null)
    }

    val docTTLs = data(Collection.DocumentTTLsField) map { d =>
      Member(Member.Kind.DocumentTTLs, Config.Bool(d, Span.Null), Span.Null)
    }

    val alias0 = SchemaNames.findAlias(scope, data)
    val alias = alias0 map { alias =>
      Annotation(Annotation.Kind.Alias, Config.Id(Name(alias, Span.Null)), Span.Null)
    }

    SchemaItem.Collection(
      Name(collName, Span.Null),
      alias,
      fields,
      migrations,
      indexes,
      uniques,
      checks,
      hd,
      ttld,
      docTTLs,
      Span.Null)
  }

  def translateFunctions(
    scope: ScopeID,
    colls: Seq[SchemaItem.Collection] = Seq.empty) = {
    val collNames = colls.map(_.name.str).toSet

    UserFunctionID
      .getAllUserDefined(scope)
      .flattenT
      .flatMap { ids =>
        val functionsQ = ids
          .map { id => SchemaCollection.UserFunction(scope).getVersionLiveNoTTL(id) }
          .sequence
          .map { _.flatten }

        functionsQ.map { versions =>
          val funcNames = versions.map(SchemaNames.findName(_)).toSet

          versions.flatMap { live =>
            translateFunction(scope, live.data, funcNames, collNames)
          }
        }
      }
  }

  private def translateFunction(
    scope: ScopeID,
    data: Data,
    funcNames: Set[String],
    collNames: Set[String]) = data(UserFunction.BodyField) match {
    case Left(src) =>
      val funcName = SchemaNames.findName(scope, data)

      Parser.lambdaExpr(src, Src.UserFunc(funcName)) match {
        case FQLResult.Ok(lambda) =>
          val alias0 = SchemaNames.findAlias(scope, data)
          val alias = if (alias0.isEmpty && collNames.contains(funcName)) {
            // Make an alias, and make sure its unique.
            var name = funcName + "Function"
            while (funcNames.contains(name) || collNames.contains(name)) {
              name += "Function"
            }
            Some(name)
          } else {
            alias0
          }

          val aliasItem = alias map { alias =>
            Annotation(
              Annotation.Kind.Alias,
              Config.Id(Name(alias, Span.Null)),
              Span.Null)
          }

          val role0 = data(UserFunction.RoleField).flatMap {
            case r: Key.Role.Builtin  => Some(r.name)
            case Key.UserRoles(roles) => roles.head.swap.toOption
          }
          val role = role0 map { r0 =>
            Annotation(
              Annotation.Kind.Role,
              Config.Id(Name(r0, Span.Null)),
              Span.Null)
          }

          val signature = data(UserFunction.UserSigField).flatMap { sig =>
            Parser.typeExpr(sig, Src.UserFunc(funcName)).toOption.collect {
              case lambda: TypeExpr.Lambda => lambda
            }
          }
          val paramTypes =
            signature.fold(Vector.empty[TypeExpr]) {
              _.params.view
                .map { case (_, te) => te }
                .toVector
            }

          val params =
            lambda.params.view.zipWithIndex.map { case (name, i) =>
              SchemaItem.Function.Arg(
                name.getOrElse(Name("_", Span.Null)),
                paramTypes.lift(i),
                variadic = false
              )
            }.toSeq

          val vari = lambda.vari map { name =>
            SchemaItem.Function.Arg(
              name.getOrElse(Name("_", Span.Null)),
              signature.flatMap { _.variadic.map { case (_, ty) => ty } },
              variadic = true
            )
          }

          val sig =
            SchemaItem.Function.Sig(
              params ++ vari,
              signature map { _.ret },
              Span.Null
            )

          val body = lambda.body match {
            case b: Expr.Block => b
            case other         => Expr.Block(Seq(Expr.Stmt.Expr(other)), Span.Null)
          }

          Some(
            SchemaItem
              .Function(
                Name(funcName, Span.Null),
                aliasItem,
                role,
                sig,
                body,
                Span.Null))

        // Unreachable, as syntax is checked before this.
        case FQLResult.Err(e) =>
          throw new IllegalStateException(
            s"failed to parse stored function body $funcName: $e")
      }

    // Skip v4 functions
    case Right(_) => None
  }

  def translateRoles(scope: ScopeID) =
    RoleID
      .getAllUserDefined(scope)
      .flattenT
      .flatMap { ids =>
        ids
          .map { id =>
            SchemaCollection.Role(scope).getVersionLiveNoTTL(id).flatMapT { live =>
              if (Role.isV10Role(live.data)) {
                Query.some(translateRole(scope, live.data))
              } else {
                Query.none
              }
            }
          }
          .sequence
          .map { _.flatten }
      }

  private def translateRole(scope: ScopeID, data: Data) = {
    val privileges = data(Role.PrivilegesField) map { pr =>
      val actions0 = pr.actions
        .selectT {
          // Exclude disabled privileges (equivalent to unset).
          case (_, StaticRoleAction(allowed)) => allowed
          case _                              => true
        }
        .mapT { case (name, action) =>
          val kind = name match {
            case Member.Kind.Create.keyword           => Member.Kind.Create
            case Member.Kind.CreateWithId.keyword     => Member.Kind.CreateWithId
            case Member.Kind.Delete.keyword           => Member.Kind.Delete
            case Member.Kind.Read.keyword             => Member.Kind.Read
            case Member.Kind.Write.keyword            => Member.Kind.Write
            case Member.Kind.HistoryRead.keyword      => Member.Kind.HistoryRead
            case Member.Kind.HistoryWrite.keyword     => Member.Kind.HistoryWrite
            case Member.Kind.UnrestrictedRead.keyword => Member.Kind.UnrestrictedRead
            case Member.Kind.Call.keyword             => Member.Kind.Call
            case other =>
              throw new IllegalStateException(s"invalid action $other")
          }

          val predCfg = action match {
            case StaticRoleAction(_) =>
              Config.Opt.empty[Config.Predicate]
            case DynamicRoleAction(Left(fql)) =>
              Config.Opt.some(Config.Predicate(parseLambdaOrThrow(fql), Span.Null))
            case _ => throw new IllegalStateException("expected v10 role")
          }

          Member(kind, predCfg, Span.Null)
        }

      val name = pr.resource match {
        case Left(n)  => n
        case Right(_) => throw new IllegalStateException("expected v10 role")
      }

      val actions =
        actions0.fold(Seq.empty[Member.OptT[Config.Predicate]]) {
          _.toSeq
        }

      Member.Named(
        Name(name, Span.Null),
        Member(
          Member.Kind.Privileges,
          SchemaItem.Role.PrivilegeConfig(actions, Span.Null),
          Span.Null))
    }

    val membership = data(Role.MembershipField) map { memb =>
      val pred = memb.predicate match {
        case None =>
          Config.Opt.empty[Config.Predicate]
        case Some(Left(fql)) =>
          Config.Opt.some(Config.Predicate(parseLambdaOrThrow(fql), Span.Null))
        case _ => throw new IllegalStateException("expected v10 role")
      }

      val name = memb.resource match {
        case Left(n)  => n
        case Right(_) => throw new IllegalStateException("expected v10 role")
      }

      Member.Named(
        Name(name, Span.Null),
        Member(Member.Kind.Membership, pred, Span.Null))
    }

    val roleName = SchemaNames.findName(scope, data)
    SchemaItem.Role(Name(roleName, Span.Null), privileges, membership, Span.Null)
  }

  def translateAPs(scope: ScopeID) =
    AccessProviderID
      .getAllUserDefined(scope)
      .flattenT
      .flatMap { ids =>
        ids
          .map { id =>
            SchemaCollection
              .AccessProvider(scope)
              .getVersionLiveNoTTL(id)
              .flatMapT { live =>
                if (isV10AP(live.data)) {
                  translateAP(live.data, scope).map { Some(_) }
                } else {
                  Query.none
                }
              }
          }
          .sequence
          .map { _.flatten }
      }

  private def isV10AP(data: Data) = {
    data(AccessProvider.RolesField).roles forall {
      _.forall { persisted =>
        // Only allow v10 functions, which are Left(String).
        persisted.predicate.forall { _.isLeft }
      }
    }
  }

  // Unfortunately, we have to drop in the Query monad to get role names.
  private def translateAP(
    data: Data,
    scope: ScopeID): Query[SchemaItem.AccessProvider] = {
    val issuer0 = data(AccessProvider.IssuerField)
    val issuer =
      Member(Member.Kind.Issuer, Config.Str(issuer0, Span.Null), Span.Null)

    val jwksURI0 = data(AccessProvider.JwksUriField)
    val jwksURI = Member(
      Member.Kind.JWKSURI,
      Config.Str(jwksURI0.toString, Span.Null),
      Span.Null)

    val roles = data(AccessProvider.RolesField).roles.getOrElse(Vector.empty)
    val rpsQ =
      roles.map { persisted =>
        val nameQ = persisted.role match {
          case Left(name) => Query.value(name)
          case Right(id) =>
            Role.get(scope, id) map {
              case Some(role) => role.name

              // Mismatched v4 ref. Use an empty string, which won't parse, but at
              // least this won't blow up.
              case None => ""
            }
        }

        nameQ.map { name =>
          persisted.predicate match {
            case None =>
              Member.Named(
                Name(name, Span.Null),
                Member(
                  Member.Kind.Role,
                  Config.Opt.empty[Config.Predicate],
                  Span.Null))

            case Some(Left(src)) =>
              val expr = parseLambdaOrThrow(src)
              Member.Named(
                Name(name, Span.Null),
                Member(
                  Member.Kind.Role,
                  Config.Opt.some(Config.Predicate(expr, Span.Null)),
                  Span.Null))

            // isV10AP guards against this
            case Some(Right(_)) =>
              throw new IllegalStateException("expected v10 predicate body")

          }
        }
      }.sequence

    val apName = SchemaNames.findName(scope, data)
    rpsQ map { rps =>
      SchemaItem.AccessProvider(
        Name(apName, Span.Null),
        issuer,
        jwksURI,
        rps,
        Span.Null)
    }
  }

  // We never use these migrations at runtime, their only purpose is to get
  // translated to schema. So we don't have fields for migrations.
  private def translateMigrationBlock(data: Data) = {
    def ise() = throw new IllegalStateException(s"invalid migration block in $data")

    val ms = data(Collection.MigrationsField) match {
      case Some(ArrayV(migrations)) =>
        migrations.map(translateMigrationItem)

      case None => Seq.empty

      case _ => ise()
    }

    Option.when(ms.nonEmpty)(
      Member.Typed(Member.Kind.Migrations, MigrationBlock(ms, Span.Null), Span.Null))
  }

  private def translateMigrationItem(item: IRValue) = {
    def ise() = throw new IllegalStateException(s"invalid migration item $item")

    def parseField(v: IRValue): Path =
      v match {
        case StringV(field) =>
          val path = FieldPath(field).getOrElse(ise())
          Path.fromList(path)

        case _ => ise()
      }

    item match {
      case m: MapV =>
        m.elems.headOption match {
          case Some(("backfill", backfill: MapV)) =>
            val field = parseField(backfill.get(List("field")).getOrElse(ise()))
            val value = backfill.get(List("value")) match {
              case Some(StringV(src)) => FSL.Lit(parseExprOrThrow(src))
              case _                  => ise()
            }

            MigrationItem.Backfill(field, value, Span.Null)

          case Some(("drop", backfill: MapV)) =>
            val field = parseField(backfill.get(List("field")).getOrElse(ise()))

            MigrationItem.Drop(field, Span.Null)

          case Some(("split", backfill: MapV)) =>
            val field = parseField(backfill.get(List("field")).getOrElse(ise()))

            val to = backfill.get(List("to")) match {
              case Some(ArrayV(items)) => items.map(parseField)
              case _                   => ise()
            }

            MigrationItem.Split(field, to, Span.Null)

          case Some(("move", backfill: MapV)) =>
            val field = parseField(backfill.get(List("field")).getOrElse(ise()))
            val to = parseField(backfill.get(List("to")).getOrElse(ise()))

            MigrationItem.Move(field, to, Span.Null)

          case Some(("add", add: MapV)) =>
            val field = parseField(add.get(List("field")).getOrElse(ise()))

            MigrationItem.Add(field, Span.Null)

          case Some(("move_conflicts", add: MapV)) =>
            val field = parseField(add.get(List("into")).getOrElse(ise()))

            MigrationItem.MoveWildcardConflicts(field, Span.Null)

          case Some(("move_wildcard", add: MapV)) =>
            val field = parseField(add.get(List("into")).getOrElse(ise()))

            MigrationItem.MoveWildcard(field, Span.Null)

          case Some(("add_wildcard", _: MapV)) =>
            MigrationItem.AddWildcard(Span.Null)

          case _ => ise()
        }

      case _ => ise()
    }
  }
}
