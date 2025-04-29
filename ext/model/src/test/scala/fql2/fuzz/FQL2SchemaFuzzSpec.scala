package fauna.model.test

import fauna.auth.Auth
import fauna.model.schema.{ GlobalNamespaceValidator, Result, SchemaTranslator }
import fql.ast._
import fql.ast.display._
import fql.parser.{ Parser, Tokens }
import org.scalacheck.{ Gen, Shrink }
import org.scalacheck.util.Pretty
import scala.collection.mutable.{ Map => MMap }
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

abstract class FQL2SchemaFuzzSpec(name: String) extends FQL2BaseFuzzSpec(name) {
  def validateModelCollection(auth: Auth, coll: SchemaItem.Collection) = {
    val items = ctx ! SchemaTranslator.translateCollections(auth.scopeID)
    if (items.sizeIs != 1) throw new IllegalStateException("too many collections")
    val translated = items.head
    if (!collectionsEqual(coll, translated)) {
      throw new IllegalStateException(
        s"expected ${coll.display}, got ${translated.display}")
    }
  }

  def collectionsEqual(a: SchemaItem.Collection, b: SchemaItem.Collection) = {
    val a0 = a.copy(
      fields = a.fields.sortBy(_.name.str),
      indexes = a.indexes.sortBy(_.name.str))
    val b0 = b.copy(
      fields = b.fields.sortBy(_.name.str),
      indexes = b.indexes.sortBy(_.name.str))

    a0.display.trim == b0.display.trim
  }

  // This trait helps us work around some issues where a field is declared and then
  // consumed by a split, which causes the fuzz migrator to assign different field
  // values than the system because the fuzz migrator sees a default for the field.
  sealed trait Source

  object Source {
    case object Field extends Source
    case object Add extends Source
  }

  case class FieldValue(expr: Expr, source: Source)

  case class State(schema: SchemaItem.Collection, fields: Map[String, FieldValue]) {
    // Return the fields as an expression, removing nulls.
    def row =
      Expr.Object(
        fields.toSeq
          .map { case (s, FieldValue(e, _)) =>
            Name(s, Span.Null) -> e
          }
          .filter { entry =>
            entry match {
              case (_, Expr.Lit(Literal.Null, _)) => false
              case _                              => true
            }
          },
        Span.Null
      )
  }

  // Generators

  // A migrator is a function that generates one migration within a block and
  // applies that migration to the migration state, which means
  // 1. applying the migration to the schema,
  // 2. migrating the row, and
  // 3. adding and removing indexes in some cases.
  type Migrator = State => Gen[State]

  // Wraps a migrator, making it into an add if the current schema is empty.
  // This allows migrations that require non-emptiness to assume schemas are
  // non-empty.
  def guardFromEmpty(m: Migrator): Migrator = state =>
    if (state.schema.fields.isEmpty) {
      add(state)
    } else {
      m(state)
    }

  // Helper to add migrations to the migration block.
  def appendMigration(
    bl: Option[Member.Typed[MigrationBlock]],
    items: Iterable[MigrationItem]): Member.Typed[MigrationBlock] = {
    val ms = bl.fold(Seq.empty[MigrationItem])(_.config.items)
    Member.Typed(
      Member.Kind.Migrations,
      MigrationBlock(ms ++ items, Span.Null),
      Span.Null)
  }

  // Generate a stringified value for `fld`, which must be a defined field
  // of a supported type.
  def valueString(fld: Field): Gen[String] =
    fld.ty.get match {
      case TypeExpr.Id("Int", _)    => Gen.choose(-1000, 1000).map(_.toString)
      case TypeExpr.Id("String", _) => Gen.alphaStr.map(s => s"\"$s\"")
      // case TypeExpr.Id("Time", _)   => Gen.const("Time.now()")
      // Int is part of the union (for now).
      case _: TypeExpr.Union => Gen.choose(-1000, 1000).map(_.toString)
      case _                 => fail(s"unsupported field type: ${fld.ty.get}")
    }

  // Helper to transform a value string from valueString into an FSL literal.
  def exprFromValueString(vs: String) = Parser.expr(vs).toOption.get

  // Generates a backfill value when `fld` doesn't have a default.
  def backfill(fld: Field): Gen[Option[MigrationItem.Backfill]] =
    for {
      vs <- valueString(fld)
    } yield {
      Option.when(fld.value.isEmpty) {
        MigrationItem.Backfill(
          Path(fld.name.str),
          FSL.Lit(exprFromValueString(vs)),
          Span.Null)
      }
    }

  def addIndex(
    schema: SchemaItem.Collection,
    term: String): SchemaItem.Collection = {
    val terms = Member(
      Member.Kind.Terms,
      Config.Seq(Seq(Config.IndexTerm(Path(term), true, Span.Null)), Span.Null),
      Span.Null)
    val cfg = SchemaItem.Collection.IndexConfig(Some(terms), None, Span.Null)
    schema.copy(indexes = schema.indexes :+ Member.Named(
      Name(s"by$term", Span.Null),
      Member(Member.Kind.Index, cfg, Span.Null)))
  }

  def dropIndex(
    schema: SchemaItem.Collection,
    term: String): (SchemaItem.Collection, Boolean) = {
    val before = schema.indexes.size
    val idxs = schema.indexes.filterNot(_.name.str != s"by$term")
    val after = idxs.size
    (
      schema.copy(indexes = schema.indexes.filterNot(_.name.str == s"by$term")),
      after < before)
  }

  def moveIndex(
    schema: SchemaItem.Collection,
    old: String,
    neuu: String): SchemaItem.Collection = {
    val (dropSchema, wasDropped) = dropIndex(schema, old)
    if (wasDropped) {
      addIndex(dropSchema, neuu)
    } else {
      dropSchema
    }

  }

  val idxThreshold = 7

  // Adds a field to `schema`, with a backfill if necessary.
  def add(state: State): Gen[State] =
    for {
      name <- fieldName.filter(n => !state.schema.fields.exists(_.name.str == n))
      fld  <- generateDefinedField(name)
      bf   <- backfill(fld)
      idxT <- Gen.choose(1, 10)
    } yield {
      val schema = state.schema.copy(
        fields = state.schema.fields :+ fld,
        migrations = Some(
          appendMigration(
            state.schema.migrations,
            Seq(MigrationItem.Add(Path(fld.name.str), Span.Null)) ++ bf)))
      val mfields = state.fields.to(MMap)
      if (bf.nonEmpty) {
        mfields += fld.name.str -> FieldValue(bf.get.value.expr, Source.Field)
      } else if (fld.value.nonEmpty) {
        fld.value.get.config match {
          case Config.Expression(e) =>
            mfields += fld.name.str -> FieldValue(e, Source.Add)
          case _ => // OK.
        }
      }
      if (idxT > idxThreshold) {
        state.copy(schema = addIndex(schema, fld.name.str), fields = mfields.to(Map))
      } else {
        state.copy(schema = schema, fields = mfields.to(Map))
      }
    }

  // Generates a drop from `schema`.
  def drop(state: State): Gen[State] =
    for {
      booted <- fieldOf(state.schema)
    } yield {
      // To avoid dealing with the wildcard, don't drop a field if the schema would
      // become empty.
      // TODO: Could have an "anchor" field instead...
      val fs = state.schema.fields.filterNot(_.name.str == booted.name.str)
      if (fs.isEmpty) {
        state
      } else {
        val mfields = state.fields.to(MMap)
        mfields -= booted.name.str
        state.copy(
          schema = dropIndex(
            state.schema.copy(
              fields = fs,
              migrations = Some(
                appendMigration(
                  state.schema.migrations,
                  Some(MigrationItem.Drop(Path(booted.name.str), Span.Null))))),
            booted.name.str
          )._1,
          fields = mfields.to(Map)
        )
      }
    }

  def isNullExpr(e: Expr) = e match {
    case Expr.Lit(Literal.Null, _) => true
    case _                         => false
  }

  // Generates a split from a compatible field, or no-ops.
  // A compatible field must be a union, which `generateDefinedField` is tweaked to
  // generate sometimes.
  def split(state: State): Gen[State] =
    for {
      // Split into two because the unions are binary!
      // TODO: Could split into the source field too.
      right <- availableFieldName(state.schema)
      left  <- availableFieldName(state.schema).retryUntil(_ != right)
      // Split one of the union fields from base that is still present in the schema,
      // or trigger a no-op.
      splatOpt <- {
        val eligible = state.schema.fields.filter { f =>
          f.ty.get match {
            case TypeExpr.Union(_, _) => true
            case _                    => false
          }
        }
        if (eligible.nonEmpty) {
          Gen.some(Gen.oneOf(eligible))
        } else {
          Gen.const(None)
        }
      }
    } yield
      if (splatOpt.isDefined) {
        val splat = splatOpt.get
        val rightF =
          Field.Defined(
            Name(right, Span.Null),
            SchemaTypeExpr.Simple(TypeExpr.Id("String", Span.Null)),
            Some(Config.Expression(Expr.Lit(Literal.Str("hello"), Span.Null))),
            Span.Null
          )
        val leftF = Field.Defined(
          Name(left, Span.Null),
          SchemaTypeExpr.Simple(
            TypeExpr.Nullable(TypeExpr.Id("Int", Span.Null), Span.Null, Span.Null)),
          Some(Config.Expression(Expr.Lit(Literal.Int(0), Span.Null))),
          Span.Null
        )
        // Source split goes out and the targets go in.
        // Work around ENG-XXX by putting the string second.
        val fields =
          state.schema.fields.filterNot(
            _.name.str == splat.name.str) :+ leftF :+ rightF
        val migrations =
          appendMigration(
            state.schema.migrations,
            Some(
              MigrationItem
                .Split(
                  Path(splat.name.str),
                  Seq(Path(leftF.name.str), Path(rightF.name.str)),
                  Span.Null)))
        val mfields = state.fields.to(MMap)
        // Drop the value if it came from an add and replace it with the
        // default/backfill. The fuzzer sees a value for the field but the system
        // never will.
        mfields += leftF.name.str -> (state.fields.get(splat.name.str) match {
          case Some(fv @ FieldValue(e, Source.Field)) if !isNullExpr(e) => fv
          case Some(FieldValue(_, Source.Add)) =>
            FieldValue(leftF.value.get.value, Source.Field)
          case _ => FieldValue(Expr.Lit(Literal.Null, Span.Null), Source.Field)
        })
        mfields += rightF.name.str -> FieldValue(
          Expr.Lit(Literal.Str("hello"), Span.Null),
          Source.Field)
        mfields -= splat.name.str
        state.copy(
          schema = dropIndex(
            state.schema.copy(fields = fields, migrations = Some(migrations)),
            splat.name.str)._1,
          fields = mfields.to(Map)
        )
      } else {
        state
      }

  // Generates a schema from `schema` with a move migration.
  def move(state: State): Gen[State] = {
    for {
      moved <- fieldOf(state.schema)
      // Don't allow renaming to an existing name.
      newName <- fieldName.retryUntil { s =>
        !state.schema.fields.map(_.name.str).toSet.contains(s)
      }
    } yield {
      val migrations = appendMigration(
        state.schema.migrations,
        Some(MigrationItem.Move(Path(moved.name.str), Path(newName), Span.Null)))
      val mfields = state.fields.to(MMap)
      state.fields.get(moved.name.str).foreach { e =>
        mfields += newName -> e
      }
      mfields -= moved.name.str
      state.copy(
        schema = moveIndex(
          state.schema.copy(
            fields = state.schema.fields.map { fld =>
              fld match {
                case f: Field.Defined =>
                  if (f.name.str == moved.name.str)
                    f.copy(name = Name(newName, Span.Null))
                  else f
                case _ => fail()
              }
            },
            migrations = Some(migrations)
          ),
          moved.name.str,
          newName
        ),
        fields = mfields.to(Map)
      )
    }
  }

  // Migrations used by generation.
  val migrationGens = Seq(
    add(_),
    guardFromEmpty(move(_)),
    guardFromEmpty(drop(_)),
    guardFromEmpty(split(_))
  )

  // Generates a single migration, applies it to a collection, and returns the
  // resulting collection.
  def applyOneMigration(state: State): Gen[State] =
    Gen.oneOf(migrationGens) flatMap { m => m(state) }

  // Generate a schema and a migration block connecting it to the base schema.
  def applyMigrations(
    base: State,
    minMigrations: Int = 5,
    maxMigrations: Int = 15
  ): Gen[State] =
    Gen.choose(minMigrations, maxMigrations) flatMap { count =>
      (1 to count).foldLeft(Gen.const(base)) { case (currG, _) =>
        currG flatMap { curr => applyOneMigration(curr) }
      }
    }

  // Generate a base schema and apply a series of migrations to it, returning the
  // base and final schemas.
  def applySeriesOfMigrations() =
    for {
      // Have at least one field to avoid dealing with wildcards.
      base     <- generateState(1, 5)
      migrated <- applyMigrations(base)
    } yield (base, migrated)

  def generateState(minFields: Int = 0, maxFields: Int = 10): Gen[State] =
    for {
      schema <- generateCollection(minFields, maxFields)
      row = rowFor(schema)
    } yield State(
      schema,
      row.map({ case (s, e) => (s, FieldValue(e, Source.Field)) }))

  // Generate a collection with some defined fields.
  def generateCollection(
    minFields: Int = 0,
    maxFields: Int = 10): Gen[SchemaItem.Collection] = {
    for {
      name      <- fieldName
      numFields <- Gen.choose(minFields, maxFields)
      fieldNames <- Gen.containerOfN[Set, String](
        numFields,
        fieldName
      )
      fields <- Gen.sequence(fieldNames.map(n => generateDefinedField(n)))
    } yield SchemaItem.Collection(
      name = Name(name, Span.Null),
      alias = None,
      fields = fields.asScala.toSeq,
      migrations = None,
      indexes = Seq.empty,
      uniques = Seq.empty,
      checks = Seq.empty,
      historyDays = SchemaItem.Collection.DefaultHistoryDays,
      ttlDays = None,
      documentTTLs = None,
      span = Span.Null
    )
  }

  // Generates a defined field of supported type. It may or may not have a default.
  def generateDefinedField(name: String): Gen[Field] = {
    for {
      (ty, constructorG) <- Gen.oneOf(
        TypeExpr.Id("Int", Span.Null) -> Gen.choose(-1000, 1000).map(_.toString),
        TypeExpr.Union(
          Seq(TypeExpr.Id("String", Span.Null), TypeExpr.Id("Int", Span.Null)),
          Span.Null) -> Gen.choose(-1000, 1000).map(_.toString),
        TypeExpr.Id("String", Span.Null) -> Gen.alphaStr.map(s => s"\"$s\"")
        // TypeExpr.Id("Time", Span.Null) -> Gen.const("Time.now()")
      )
      constructor <- constructorG.map { c =>
        val expr = Parser.expr(c).toOption.get
        Config.Expression(expr)
      }
      includeDefault <- Gen.oneOf(true, false)
    } yield Field.Defined(
      name = Name(name, Span.Null),
      schemaType = SchemaTypeExpr.Simple(ty),
      value = if (includeDefault) Some(constructor) else None,
      span = Span.Null
    )
  }

  // Generate a name for a field.
  def fieldName: Gen[String] = Gen.identifier.filter { id =>
    Tokens.isValidIdent(id) &&
    !Tokens.SpecialFieldNames.contains(id) &&
    !GlobalNamespaceValidator.nameConflictsWithEnvironment(id)
  }

  // Generate a name for a field not present in `schema`.
  def availableFieldName(schema: SchemaItem.Collection): Gen[String] =
    fieldName.retryUntil { s =>
      !schema.fields.exists(_.name.str == s)
    }

  // Choose a field of `schema`.
  // Assumes that the schema is non-empty.
  def fieldOf(schema: SchemaItem.Collection): Gen[Field] = Gen.oneOf(schema.fields)

  // Generate a row matching a schema.
  def rowFor(schema: SchemaItem.Collection): Map[String, Expr] = {
    val b = Map.newBuilder[String, Expr]
    schema.fields.foreach {
      case f: Field.Defined =>
        if (f.value.isEmpty) {
          b += f.name.str -> valueFor(f.schemaType.asTypeExpr)
        } else {
          // TODO: Let the system put in the default.
          b += f.name.str -> f.value.get.value
        }
      case _ => // Nothing to do.
    }
    b.result()
  }

  def valueFor(ty: TypeExpr): Expr = ty match {
    case TypeExpr.Union(_, _) =>
      // We know this is a String | Int.
      Expr.Lit(Literal.Int(11), Span.Null)
    case TypeExpr.Id("Int", _) =>
      Expr.Lit(Literal.Int(1), Span.Null)
    case TypeExpr.Id("String", _) =>
      Expr.Lit(Literal.Str("literal"), Span.Null)
    case _ =>
      throw new IllegalStateException(s"unexpected field type expr ${ty.display}")
  }

  // Shrinkers

  // TODO: Shrink migration sequences.

  implicit val shrinkCollection: Shrink[SchemaItem.Collection] = Shrink { c =>
    Shrink.shrink(c.fields).map { fields =>
      c.copy(fields = fields)
    }
  }

  // Utilities.

  // Makes errors display the AST.
  implicit def prettyCollection(c: SchemaItem.Collection): Pretty = Pretty { _ =>
    // Putting the `c.display` in here makes scalacheck not escape the newlines.
    c.display
  }

  def updateOk(auth: Auth, files: (String, String)*) = {
    updateSchema(auth, files: _*) match {
      case Result.Ok(v) => v
      case Result.Err(e) => {
        fail(s"unexpected error updating schema:\n${renderSchemaErrors(e, files)}")
      }
    }
  }
}
