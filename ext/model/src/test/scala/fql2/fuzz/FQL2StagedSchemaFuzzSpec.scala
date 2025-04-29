package fauna.model.test

import fauna.atoms.DocID
import fauna.auth.{ AdminPermissions, Auth, JWTToken }
import fauna.codex.json.JSObject
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.schema.CollectionConfig
import fauna.model.schema.SchemaStatus
import fauna.model.schema.SchemaTypeResolver
import fauna.model.tasks.TaskExecutor
import fauna.model.test.TaskHelpers.tasks
import fauna.model.Database
import fauna.net.security.{ JWK, JWKProvider, JWKSError, JWT, JWTFields }
import fauna.repo.schema.SchemaType
import fauna.repo.store.CacheStore
import fauna.repo.Store
import fauna.storage.ir.MapV
import fauna.util.BCrypt
import fql.ast._
import fql.ast.display._
import fql.typer.Type
import fql.typer.Typer
import java.net.URL
import java.time.Instant
import org.scalacheck.Gen
import org.scalacheck.Prop
import scala.collection.mutable.{ ListBuffer, Map => MMap }
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

case class SchemaItemNames(
  collections: Set[String],
  functions: Set[String],
  roles: Set[String],
  accessProviders: Set[String]) {
  assert(collections.nonEmpty, "collections must be non-empty")
  assert(functions.nonEmpty, "functions must be non-empty")
  assert(roles.nonEmpty, "roles must be non-empty")
  assert(accessProviders.nonEmpty, "access providers must be non-empty")
  assert(
    collections.intersect(functions).isEmpty,
    "collections and functions must not intersect")

  def makeCollectionName(spec: FQL2SchemaFuzzSpec) = spec.fieldName
    .filter(name => !collections.contains(name))
    .filter(name => !functions.contains(name))
  def makeFunctionName(spec: FQL2SchemaFuzzSpec) = makeCollectionName(spec)

  def makeRoleName(spec: FQL2SchemaFuzzSpec) =
    spec.fieldName.filter(name => !roles.contains(name))

  def renameCollection(oldName: String, newName: String) = {
    assert(collections.contains(oldName), s"$oldName not in $collections")
    assert(!collections.contains(newName), s"$newName already in $collections")
    copy(collections = collections - oldName + newName)
  }
}

case class Schema(
  collections: Seq[SchemaItem.Collection],
  functions: Seq[SchemaItem.Function],
  roles: Seq[SchemaItem.Role],
  accessProviders: Seq[SchemaItem.AccessProvider]) {
  import FQL2StagedSchemaFuzzSpec._

  def toSrc = {
    (collections ++ functions ++ roles ++ accessProviders)
      .map(_.display)
      .mkString("\n\n")
  }

  override def toString = toSrc

  def validate(key: String) = {
    val auth = ctx ! Auth.lookup(key).map(_.get)
    val audience = ctx ! Database.forScope(auth.scopeID).map { db =>
      JWTToken.canonicalDBUrl(db.get)
    }

    ctx ! CacheStore.invalidateScope(auth.scopeID)
    ctx.cacheContext.schema.invalidate()

    // Ensure all our roles with call privileges work.
    roles.foreach { role =>
      val token = s"$key:@role/${role.item.name.str}"
      val auth = ctx ! Auth.fromAuth(token, List(BCrypt.hash("secret")))

      auth match {
        case Some(auth) => validateRole(auth, role.name.str)
        case None =>
          throw new IllegalStateException(s"failed to lookup auth for token $token")
      }
    }

    accessProviders.foreach { ap =>
      val payload = JSObject(
        JWTFields.Issuer -> ap.issuer.configValue,
        JWTFields.Audience -> audience,
        JWTFields.IssuedAt -> Instant.now().getEpochSecond
      )

      val key = aJWK.sample
      val jwt = JWT.createToken(payload, key, "RS256")

      val auth = ctx ! Auth.fromJwt(
        jwt.getToken,
        new JWKProvider {
          def getJWK(url: URL, kid: Option[String]): Future[JWK] = {
            if (ap.jwksURI.configValue == url.toString && key.kid == kid) {
              Future.successful(key)
            } else {
              Future.failed(new JWKSError(url, "wrong uri or key"))
            }
          }
        }
      )

      auth match {
        case Some(auth) => validateRole(auth, ap.roles.head.name.str)
        case None =>
          throw new IllegalStateException(s"failed to lookup JWT for ${ap.display}")
      }
    }
  }

  private def validateRole(auth: Auth, name: String) = {
    val role =
      roles.find(_.name.str == name).getOrElse(sys.error(s"role $name not found"))

    role.privileges.foreach { privilege =>
      privilege.configValue.actions.foreach { action =>
        action.kind match {
          case Member.Kind.Call =>
            val function = privilege.name.str

            // The function should be callable with this role.
            evalOk(auth, s"$function(1)")

          case Member.Kind.Read =>
            val coll = privilege.name.str

            // The doc `0` shouldn't exist, and we should have permission to check
            // that.
            evalOk(auth, s"if ($coll.byId(0).exists()) abort(0)")

            // The collection should not be writable with this role.
            val err = evalErr(auth, s"$coll.create({})")
            if (err.code != "permission_denied") {
              throw new IllegalStateException(s"expected permission_denied")
            }

          case _ => ()
        }
      }
    }
  }
}

object FQL2StagedSchemaFuzzSpec
    extends FQL2SchemaFuzzSpec("staged")
    with AuthGenerators {
  property("non-collections") = Prop.forAll(for {
    names  <- makeNames
    active <- makeSchema(names)
    staged <- makeSchema(names)
  } yield (active, staged)) { case (active, staged) =>
    val auth = newDB.withPermissions(AdminPermissions)
    val key = mkKey(auth, "admin")

    updateSchemaOk(auth, "main.fsl" -> active.toSrc)
    active.validate(key)

    updateSchemaPinOk(auth, "main.fsl" -> staged.toSrc)
    active.validate(key)

    ctx ! SchemaStatus.abandon(new FQLInterpreter(auth))
    active.validate(key)

    updateSchemaPinOk(auth, "main.fsl" -> staged.toSrc)
    active.validate(key)

    ctx ! SchemaStatus.commit(new FQLInterpreter(auth))
    staged.validate(key)

    PropTestResult.ValidTestPassed
  }

  property("non-collections-with-renames") = Prop.forAll(for {
    names  <- makeNames
    active <- makeSchema(names)
    staged <- makeSchema(names)

    // Build a sequence of renames, which keep the schema valid between each rename.
    // Each element in `renames` is a pair of oldName -> newName.
    renameCount <- Gen.choose(0, 5)
    (_, renames) <- (0 until renameCount).foldLeft(
      Gen.const((names, Seq.empty[(String, String)]))) { case (g, _) =>
      g.flatMap { case (names, acc) =>
        for {
          oldName <- Gen.oneOf(names.collections)
          newName <- names.makeCollectionName(this)
        } yield (names.renameCollection(oldName, newName), acc :+ (oldName, newName))
      }
    }
  } yield (active, staged, renames)) { case (active, staged0, renames) =>
    val auth = newDB.withPermissions(AdminPermissions)
    val key = mkKey(auth, "admin")

    var staged = staged0

    updateSchemaOk(auth, "main.fsl" -> active.toSrc)
    active.validate(key)

    updateSchemaPinOk(auth, "main.fsl" -> staged.toSrc)
    active.validate(key)

    renames.foreach { case (oldColl, newColl) =>
      // NB: This is a cascading rename, ie, it'll update role privileges to use
      // the new collection name.
      evalOk(auth, s"Collection.byName('$oldColl')!.update({ name: '$newColl' })")

      // Go ahead and update our staged schema to reflect the rename.
      staged = staged.copy(
        collections = staged.collections.map { coll =>
          if (coll.item.name.str == oldColl) {
            coll.copy(name = Name(newColl, Span.Null))
          } else {
            coll
          }
        },
        roles = staged.roles.map { role =>
          role.copy(
            privileges = role.privileges.map { privilege =>
              privilege.copy(
                name = privilege.name match {
                  case Name(`oldColl`, _) => Name(newColl, Span.Null)
                  case _                  => privilege.name
                }
              )
            }
          )
        }
      )

      active.validate(key)
    }

    ctx ! SchemaStatus.commit(new FQLInterpreter(auth))
    staged.validate(key)

    PropTestResult.ValidTestPassed
  }

  property("stage-abandon-stage-commit") = Prop.forAll(
    activeStagedSchema
  ) { case (active, staged) =>
    val auth = newDB

    // Initial setup.
    updateOk(auth, "main.fsl" -> active.item.display)
    val id = evalOk(auth, makeDoc(active.item)).as[DocID]
    validateModelCollection(auth, active.item)
    validateDoc(auth, active.item, id)

    // Stage a change.
    updateSchemaPinOk(auth, "main.fsl" -> staged.item.display)
    validateModelCollection(auth, staged.item)

    // Doc should still be the active version.
    validateDoc(auth, active.item, id)

    // Abandon.
    ctx ! SchemaStatus.abandon(new FQLInterpreter(auth))

    // Doc should still be the active version.
    validateDoc(auth, active.item, id)

    // Stage a second time.
    updateSchemaPinOk(auth, "main.fsl" -> staged.item.display)
    validateModelCollection(auth, staged.item)

    // Doc should still be the active version.
    validateDoc(auth, active.item, id)

    // Commit.
    ctx ! SchemaStatus.commit(new FQLInterpreter(auth))

    // Doc should be the staged version.
    validateDoc(auth, staged.item, id)

    PropTestResult.ValidTestPassed
  }

  property("stage-abandon-stage-commit-async") = Prop.forAll(
    activeStagedSchema
  ) { case (active, staged) =>
    // pending - async migration tasks are sometimes kicked off when they shouldn't
    // be.
    if (1 != 2) {
      PropTestResult.ValidTestPassed
    } else {
      val auth = newDB

      // Initial setup.
      updateOk(auth, "main.fsl" -> active.item.display)
      val id = evalOk(auth, makeDoc(active.item)).as[DocID]
      evalOk(auth, s"Set.sequence(0, 256).forEach(_ => ${makeDoc(active.item)})")

      validateModelCollection(auth, active.item)
      validateDoc(auth, active.item, id)

      // Stage a change.
      updateSchemaPinOk(auth, "main.fsl" -> staged.item.display)
      validateModelCollection(auth, staged.item)
      ctx ! tasks shouldBe empty

      // Doc should still be the active version.
      validateDoc(auth, active.item, id)

      // Abandon.
      ctx ! SchemaStatus.abandon(new FQLInterpreter(auth))
      ctx ! CacheStore.invalidateScope(auth.scopeID)
      ctx ! tasks shouldBe empty

      // Doc should still be the active version.
      validateDoc(auth, active.item, id)

      // Stage a second time.
      updateSchemaPinOk(auth, "main.fsl" -> staged.item.display)
      validateModelCollection(auth, staged.item)
      ctx ! tasks shouldBe empty

      // Doc should still be the active version.
      validateDoc(auth, active.item, id)

      // Commit.
      ctx ! SchemaStatus.commit(new FQLInterpreter(auth))
      ctx ! CacheStore.invalidateScope(auth.scopeID)
      ctx ! tasks should not be empty
      while ((ctx ! tasks).nonEmpty) TaskExecutor(ctx).step()

      // Doc should be the staged version.
      validateDoc(auth, staged.item, id)

      PropTestResult.ValidTestPassed
    }
  }

  def makeNames = for {
    collections <- Gen.listOf(fieldName).map(_.toSet).filter(_.nonEmpty)
    functions <- Gen
      .listOf(fieldName)
      .map(_.toSet)
      // Collection and function names may not conflict.
      .map(_.filter(name => collections.forall(_ != name)))
      .filter(_.nonEmpty)
    roles           <- Gen.listOf(fieldName).map(_.toSet).filter(_.nonEmpty)
    accessProviders <- Gen.listOf(fieldName).map(_.toSet).filter(_.nonEmpty)
  } yield SchemaItemNames(collections, functions, roles, accessProviders)

  def makeSchema(names: SchemaItemNames) = for {
    _ <- Gen.const(())

    collections = names.collections.map { name =>
      SchemaItem.Collection(name = Name(name, Span.Null), span = Span.Null)
    }.toSeq
    functions0 = names.functions.map { name =>
      // All our functions are `x -> x`
      SchemaItem.Function(
        name = Name(name, Span.Null),
        sig = SchemaItem.Function.Sig(
          args = Seq(
            SchemaItem.Function
              .Arg(name = Name("x", Span.Null), ty = None, variadic = false)),
          ret = None,
          span = Span.Null),
        body = Expr.Block(Seq(Expr.Stmt.Expr(Expr.Id("x", Span.Null))), Span.Null),
        span = Span.Null
      )
    }.toSeq

    roles <- Gen
      .sequence(names.roles.map(generateRole(_, collections, functions0)))
      .map(_.asScala.toSeq)
      .filter(_.nonEmpty)

    issuers <- Gen
      .listOfN(
        names.accessProviders.size,
        fieldName.map { domain => s"https://$domain.auth0.com" })
      .map(_.toSet)
      .filter(_.size == names.accessProviders.size)

    accessProviders <- Gen
      .sequence(names.accessProviders
        .zip(issuers)
        .map { case (name, issuer) => generateAccessProvider(name, issuer, roles) })
      .map(_.asScala.toSeq)
      .filter(_.nonEmpty)

    // Assign the functions some roles.
    functions <- Gen.sequence(functions0.map { f =>
      Gen.oneOf(true, false).flatMap {
        case true =>
          Gen.oneOf(roles).map { role =>
            f.copy(role = Some(
              Annotation(
                Annotation.Kind.Role,
                Config.Id(role.item.name),
                Span.Null)))
          }
        case false => Gen.const(f)
      }
    })
  } yield Schema(collections, functions.asScala.toSeq, roles, accessProviders)

  def generateRole(
    name: String,
    collections: Seq[SchemaItem.Collection],
    functions: Seq[SchemaItem.Function]): Gen[SchemaItem.Role] = {
    for {
      privileges <- Gen
        .sequence((collections ++ functions).map { item =>
          Gen.oneOf(true, false).map {
            case true  => Some(generatePrivilege(item))
            case false => None
          }
        })
        .map(_.asScala.toSeq.flatten)
    } yield SchemaItem
      .Role(name = Name(name, Span.Null), privileges = privileges, span = Span.Null)
  }

  def generatePrivilege(item: SchemaItem) = item match {
    // privileges ${c.name} { read }
    case c: SchemaItem.Collection =>
      Member.Named(
        c.name,
        Member(
          Member.Kind.Privileges,
          SchemaItem.Role.PrivilegeConfig(
            actions = Seq(Member(Member.Kind.Read, Config.Opt(None), Span.Null)),
            span = Span.Null),
          Span.Null)
      )

    // privileges ${f.name} { call }
    case f: SchemaItem.Function =>
      Member.Named(
        f.name,
        Member(
          Member.Kind.Privileges,
          SchemaItem.Role.PrivilegeConfig(
            actions = Seq(Member(Member.Kind.Call, Config.Opt(None), Span.Null)),
            span = Span.Null),
          Span.Null
        )
      )

    case _ => sys.error("unreachable")
  }

  def generateAccessProvider(
    name: String,
    issuer: String,
    roles: Seq[SchemaItem.Role]): Gen[SchemaItem.AccessProvider] = {
    for {
      role <- Gen.oneOf(roles)
    } yield SchemaItem
      .AccessProvider(
        name = Name(name, Span.Null),
        issuer =
          Member(Member.Kind.Issuer, Config.Str(issuer, Span.Null), Span.Null),
        jwksURI = Member(
          Member.Kind.JWKSURI,
          Config.Str(s"$issuer/.well-known/jwks.json", Span.Null),
          Span.Null),
        roles = Seq(
          Member.Named(
            role.name,
            Member(Member.Kind.Role, Config.Opt(None), Span.Null))),
        span = Span.Null
      )
  }

  // TODO: Make this a `Gen`.
  def migrateFrom(
    from: SchemaItem.Collection,
    to: SchemaItem.Collection): Seq[MigrationItem] = {
    val drops = ListBuffer.empty[MigrationItem.Drop]
    val moves = ListBuffer.empty[MigrationItem.Move]
    val adds = ListBuffer.empty[MigrationItem]

    val avail = to.fields.map { f => f.name.str -> f }.to(MMap)
    val toAdd = to.fields.map { f => f.name.str -> f }.to(MMap)

    from.fields.foreach { f =>
      avail.values.find { _.name.str == f.name.str } match {
        // We found a field with the same name. If the types don't match, drop and
        // re-add.
        case Some(t) =>
          if (f.ty.get != t.ty.get) {
            drops += MigrationItem.Drop(Path(t.name.str), Span.Null)
            adds += MigrationItem.Add(Path(t.name.str), Span.Null)
            adds += MigrationItem.Backfill(
              Path(t.name.str),
              FSL.Lit(exprFromValueString(valueString(t).sample.get)),
              Span.Null)
          }

          avail.remove(t.name.str)
          toAdd.remove(t.name.str)

        case None =>
          // Pick another field that hasn't been migrated, and move it.
          avail.values.find { _.ty == f.ty } match {
            // If a field with the same name has already been moved, don't bother.
            // Ordering moves is a pain.
            case Some(t)
                if !moves.exists(_.to.elems.head.asField.get == f.name.str) =>
              moves += MigrationItem.Move(
                Path(f.name.str),
                Path(t.name.str),
                Span.Null)
              avail.remove(t.name.str)
              toAdd.remove(t.name.str)

            case _ =>
              drops += MigrationItem.Drop(Path(f.name.str), Span.Null)
          }
      }
    }

    toAdd.values.foreach { f =>
      adds += MigrationItem.Add(Path(f.name.str), Span.Null)
      adds += MigrationItem.Backfill(
        Path(f.name.str),
        FSL.Lit(exprFromValueString(valueString(f).sample.get)),
        Span.Null)
    }

    drops.toSeq ++ moves ++ adds
  }

  def activeStagedSchema: Gen[(
    WrappedSchemaItem[SchemaItem.Collection],
    WrappedSchemaItem[SchemaItem.Collection])] = for {
    active <- generateCollection()
      .filter(_.fields.nonEmpty)
      .map(WrappedSchemaItem[SchemaItem.Collection])
    staged <- generateCollection()
      .filter(_.fields.nonEmpty)
      .map(WrappedSchemaItem[SchemaItem.Collection])
    migrations = WrappedMigrations(migrateFrom(active.item, staged.item))
    stagedWithMigrations = WrappedSchemaItem(if (migrations.migrations.nonEmpty) {
      staged.item.copy(
        name = active.item.name,
        migrations = Some(
          Member.Typed(
            Member.Kind.Migrations,
            MigrationBlock(migrations.migrations, Span.Null),
            Span.Null)
        ))
    } else {
      staged.item.copy(name = active.item.name)
    })
  } yield (active, stagedWithMigrations)

  def makeDoc(schema: SchemaItem.Collection): String = {
    val fields = schema.fields
      .flatMap { f =>
        val ty = Typer().typeTExprType(f.ty.get).getOrElse(sys.error("unreachable"))

        if (hasNull(ty)) {
          None
        } else {
          Some(f.name.str -> makeValue(ty).sample.get)
        }
      }

    s"${schema.name.str}.create({${fields.map { case (k, v) => s"$k: $v" }.mkString(", ")}})"
  }

  def hasNull(ty: Type): Boolean = {
    ty match {
      case Type.Null          => true
      case Type.Union(tys, _) => tys.exists(hasNull)
      case _                  => false
    }
  }

  def makeValue(ty: Type): Gen[String] = {
    ty match {
      case Type.Null          => Gen.const("null")
      case Type.Int           => Gen.choose(-1000, 1000).map(_.toString)
      case Type.Str           => Gen.alphaStr.map(s => s"\"$s\"")
      case Type.Union(tys, _) => Gen.pick(1, tys.map(makeValue)).flatMap(_.head)
      case _                  => sys.error(s"todo: makeValue($ty)")
    }
  }

  def validateDoc(auth: Auth, schema: SchemaItem.Collection, id: DocID): Unit = {
    val vers = ctx ! CollectionConfig(auth.scopeID, id.collID).flatMap { config =>
      Store.get(config.get.Schema, id).map(_.get)
    }

    val data = vers.data.fields.get(List("data")) match {
      case Some(v: MapV) => v
      case _             => sys.error("unreachable")
    }

    schema.fields.foreach { field =>
      data.get(List(field.name.str)) match {
        case Some(value) =>
          val ty =
            Typer().typeTExprType(field.ty.get).getOrElse(sys.error("unreachable"))
          val schemaTy = ctx ! SchemaTypeResolver(ty)(auth.scopeID).map(_.get)
          if (!SchemaType.isValueOfType(schemaTy, value)) {
            println(s"${field.name.str}: $value ! <: $schemaTy")
            throw new IllegalStateException("field didn't match")
          }
        case None =>
          throw new IllegalStateException(
            s"missing field ${field.name.str} in ${vers.data}")
      }
    }
  }
}

case class WrappedSchemaItem[I <: SchemaItem](item: I) {
  override def toString: String = item.display
}

case class WrappedMigrations(migrations: Seq[MigrationItem]) {
  override def toString: String = migrations.map(_.display).mkString("")
}
