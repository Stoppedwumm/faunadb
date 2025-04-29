package fauna.model.schema.manager

import fauna.model.schema.SchemaManager
import fauna.repo.values.Value
import fql.ast._
import fql.ast.display._

trait SchemaToStruct { self: SchemaManager =>
  def toStruct(item: SchemaItem, userData: Option[Value]): Value.Struct =
    item match {
      case col: SchemaItem.Collection =>
        val struct = Value.Struct.newBuilder
        val indexes = Value.Struct.newBuilder
        val constraints = Value.Array.newBuilder
        val migrations = Value.Array.newBuilder
        val cfs = Value.Struct.newBuilder
        val pfs = Value.Struct.newBuilder

        struct += "name" -> Value.Str(item.name.str)

        col.alias foreach { alias =>
          struct += "alias" -> Value.Str(alias.configValue.str)
        }

        // History days defaults to Document.DefaultRetainDays.
        struct += "history_days" -> Value.Long(col.historyDays.configValue)

        col.ttlDays foreach { ttl =>
          struct += "ttl_days" -> Value.Long(ttl.configValue)
        }

        col.indexes foreach { index =>
          val cfg = index.configValue
          val b = Value.Struct.newBuilder

          cfg.terms.view map { _.configValue } foreach { paths =>
            b += "terms" -> Value.Array.fromSpecific(paths.view map { path =>
              Value.Struct(
                "field" -> Value.Str(path.displayTerm),
                "mva" -> Value.Boolean(path.mva)
              )
            })
          }

          cfg.values.view map { _.configValue } foreach { paths =>
            b += "values" -> Value.Array.fromSpecific(paths.view map { path =>
              Value.Struct(
                "field" -> Value.Str(path.displayValue),
                "order" -> Value.Str(if (path.asc) "asc" else "desc"),
                "mva" -> Value.Boolean(path.mva)
              )
            })
          }

          indexes += index.name.str -> b.result()
        }

        col.uniques foreach { unique =>
          constraints += Value.Struct(
            "unique" -> Value.Array.fromSpecific(
              unique.configValue.view map { path =>
                Value.Struct(
                  "field" -> Value.Str(path.displayTerm),
                  "mva" -> Value.Boolean(path.mva))
              }
            ))
        }

        col.migrations.foreach { block =>
          block.config.items.foreach {
            case MigrationItem.Backfill(path, expr, _) =>
              migrations += Value.Struct(
                "backfill" -> Value.Struct(
                  "field" -> Value.Str(displayPath(path)),
                  "value" -> Value.Str(expr.expr.display)
                )
              )

            case MigrationItem.Drop(path, _) =>
              migrations += Value.Struct(
                "drop" -> Value.Struct(
                  "field" -> Value.Str(displayPath(path))
                )
              )

            case MigrationItem.Split(field, to, _) =>
              migrations += Value.Struct(
                "split" -> Value.Struct(
                  "field" -> Value.Str(displayPath(field)),
                  "to" -> Value.Array(to.map(t => Value.Str(displayPath(t))): _*)
                )
              )

            case MigrationItem.Move(field, to, _) =>
              migrations += Value.Struct(
                "move" -> Value.Struct(
                  "field" -> Value.Str(displayPath(field)),
                  "to" -> Value.Str(displayPath(to))
                )
              )

            case MigrationItem.Add(field, _) =>
              migrations += Value.Struct(
                "add" -> Value.Struct(
                  "field" -> Value.Str(displayPath(field))
                )
              )

            case MigrationItem.MoveWildcardConflicts(into, _) =>
              migrations += Value.Struct(
                "move_conflicts" -> Value.Struct(
                  "into" -> Value.Str(displayPath(into))
                )
              )

            case MigrationItem.MoveWildcard(into, _) =>
              migrations += Value.Struct(
                "move_wildcard" -> Value.Struct(
                  "into" -> Value.Str(displayPath(into))
                )
              )

            case MigrationItem.AddWildcard(_) =>
              migrations += Value.Struct(
                // Empty struct makes it easier to add fields later.
                "add_wildcard" -> Value.Struct()
              )
          }
        }

        col.fields foreach {
          case cf: Field.Computed =>
            val struct = Value.Struct.newBuilder
            struct += "body" -> displayLambda(cf._value.expr)

            cf.ty.foreach { te =>
              struct += "signature" -> Value.Str(te.display)
            }

            cfs += cf.name.str -> struct.result()

          case f: Field.Defined =>
            val struct = Value.Struct.newBuilder
            struct += "signature" -> Value.Str(f.schemaType.display)
            f.value foreach { d =>
              struct += "default" -> Value.Str(d.value.display)
            }
            pfs += f.name.str -> struct.result()

          case Field.Wildcard(ty, _) =>
            struct += "wildcard" -> Value.Str(ty.display)
        }

        col.checks foreach { check =>
          constraints += Value.Struct(
            "check" -> Value.Struct(
              "name" -> Value.Str(check.name.str),
              "body" -> displayLambda(check.configValue)
            )
          )
        }

        val pfsRes = pfs.result()
        col.documentTTLs.foreach { d =>
          // Set document_ttls in model when it's explicitly set.
          // Otherwise, leave it empty. The value is inferred
          // from the presence or absence of defined fields.
          struct += "document_ttls" -> Value.Boolean(d.config.value)
        }

        struct += "indexes" -> indexes.result()
        struct += "constraints" -> constraints.result()
        val cfsRes = cfs.result()
        if (cfsRes.fields.nonEmpty) {
          struct += "computed_fields" -> cfsRes
        }
        if (pfsRes.fields.nonEmpty) {
          struct += "fields" -> pfsRes
        }
        val migrationsRes = migrations.result()
        if (migrationsRes.elems.nonEmpty) {
          struct += "migrations" -> migrationsRes
        }
        userData foreach { struct += "data" -> _ }
        struct.result()

      case fn: SchemaItem.Function =>
        val struct = Value.Struct.newBuilder
        struct += "name" -> Value.Str(item.name.str)
        struct += "body" -> Value.Str(fn.displayAsUDFBody)

        val sig = fn.displayAsUDFSig
        if (sig.nonEmpty) {
          struct += "signature" -> Value.Str(sig)
        }

        fn.alias foreach { alias =>
          struct += "alias" -> Value.Str(alias.configValue.str)
        }

        fn.role foreach { role =>
          struct += "role" -> Value.Str(role.configValue.str)
        }

        userData foreach { ud => struct += "data" -> ud }
        struct.result()

      case role: SchemaItem.Role =>
        val struct = Value.Struct.newBuilder
        val privileges = Value.Array.newBuilder
        val membership = Value.Array.newBuilder

        struct += "name" -> Value.Str(item.name.str)

        role.privileges foreach { priv =>
          val b = Value.Struct.newBuilder
          val cfg = priv.configValue

          b += "resource" -> Value.Str(priv.name.str)

          if (cfg.actions.nonEmpty) {
            val actions = Value.Struct.newBuilder
            cfg.actions foreach { action =>
              val pred = action.configValue.fold(Value.True: Value)(displayLambda)
              actions += action.kind.keyword -> pred
            }
            b += "actions" -> actions.result()
          }

          privileges += b.result()
        }

        role.membership foreach { mem =>
          val b = Value.Struct.newBuilder
          b += "resource" -> Value.Str(mem.name.str)
          mem.configValue.foreach { lambda =>
            b += "predicate" -> displayLambda(lambda)
          }
          membership += b.result()
        }

        struct += "privileges" -> privileges.result()
        struct += "membership" -> membership.result()
        userData foreach { ud => struct += "data" -> ud }
        struct.result()

      case ap: SchemaItem.AccessProvider =>
        val struct = Value.Struct.newBuilder
        struct += "name" -> Value.Str(ap.name.str)
        struct += "issuer" -> Value.Str(ap.issuer.configValue)
        struct += "jwks_uri" -> Value.Str(ap.jwksURI.configValue)

        if (ap.roles.nonEmpty) {
          val roles = Value.Array.newBuilder

          ap.roles foreach { role =>
            roles += (role.configValue match {
              case Some(lambda) =>
                val b = Value.Struct.newBuilder
                b += "role" -> Value.Str(role.name.str)
                b += "predicate" -> displayLambda(lambda)
                b.result()
              case None => Value.Str(role.name.str)
            })
          }

          struct += "roles" -> roles.result()
        }

        userData foreach { ud => struct += "data" -> ud }
        struct.result()

      case other =>
        throw new IllegalArgumentException(s"unsupported schema item $other")
    }

  /** Displays the lambda for a predicate field. This will display long lambdas
    * as-is, and wrap short lambdas in parenthesis.
    */
  private def displayLambda(lambda: Expr.Lambda): Value.Str =
    Value.Str(lambda match {
      case lambda: Expr.LongLambda => lambda.display
      case lambda: Expr.ShortLambda =>
        Expr.Tuple(Seq(lambda), Span.Null).display
    })

  private def displayPath(path: Path): String = {
    val d = new Displayer
    d.writePath(path)
    d.sb.result()
  }
}
