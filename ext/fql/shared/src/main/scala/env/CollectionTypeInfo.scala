package fql.env

import fql.ast._
import fql.typer._
import language.implicitConversions
import scala.collection.immutable.ArraySeq
import scala.collection.SeqMap

object CollectionTypeInfo {
  val MetaFields =
    SeqMap("id" -> Type.ID, "ts" -> Type.Time, "ttl" -> Type.Optional(Type.Time))

  val RangeType = Type.Union(
    Type.Record("from" -> Type.Any),
    Type.Record("to" -> Type.Any),
    Type.Record("from" -> Type.Any, "to" -> Type.Any))

  def nullDocName(name: String) = s"Null$name"
  def collName(name: String) = s"${name}Collection"
  def docTag(name: String) = TypeShape.DocType.Doc(name)
  def nullDocTag(name: String) = TypeShape.DocType.NullDoc(name)

  private[CollectionTypeInfo] def uncheckedType(te: TypeExpr): Type =
    Typer.typeTSchemeUncheckedType(TypeExpr.Scheme(Nil, te))

  object Precheck {
    def fromSchemaItem(
      schema: SchemaItem.Collection,
      defined: SeqMap[String, (Either[Type, SchemaTypeExpr], Boolean)],
      computed: SeqMap[String, (Type, EnvCheckResult)],
      wildcard: Option[Either[Type, TypeExpr]]
    ) = {
      val indexes = schema.indexes.view
        .map { idx =>
          idx.name.str -> idx.configValue.terms
            .map(_.configValue)
            .getOrElse(Nil)
            .map(t => (t.path, t.mva))
            .toSeq
        }
        .to(SeqMap)

      // See model/schema/CollectionConfig.scala
      // NB: If a wildcard is defined we must explicitly require `ttl` be unset.
      val hasDocTTL = schema.documentTTLs.fold(schema.fields.isEmpty)(_.config.value)

      Precheck(
        schema.name,
        schema.alias.map(_.configValue),
        hasDocTTL,
        defined,
        computed,
        wildcard,
        indexes,
        TypeShape.TypeHint.UserCollection
      )

    }
  }

  final case class Precheck(
    name: Name,
    alias: Option[Name],
    hasDocTTL: Boolean,
    // if type expr validation for a defined field fails, it will contain Left of a
    // poisoned var.
    defined: SeqMap[String, (Either[Type, SchemaTypeExpr], Boolean)],
    computed: SeqMap[String, (Type, EnvCheckResult)],
    // if type expr validation for the wildcard fails, it will contian a Left of a
    // poisoned var.
    wildcard: Option[Either[Type, TypeExpr]],
    indexes: SeqMap[String, Seq[(Path, Boolean)]],
    typeHint: TypeShape.TypeHint
  ) extends CollectionTypeInfo(
        name,
        hasDocTTL,
        defined,
        computed.view.mapValues(_._1).to(SeqMap),
        wildcard,
        indexes,
        typeHint) {

    lazy val aliased = alias.map(a => copy(name = a, alias = None))

    // Fetches the inferred computed field types off a thunk, so typechecking must
    // be completed before accessing this this.
    def getChecked() = {
      val dfs = defined.view.mapValues(t => (t._1.toOption.get, t._2))
      val cfs =
        computed.view.flatMap { case (n, (_, res)) =>
          res.get.expr match {
            case TypeExpr.Lambda(_, _, ret, _) => Some(n -> ret)
            case _                             => None
          }
        }

      Checked(
        name,
        alias,
        hasDocTTL,
        dfs.to(SeqMap),
        cfs.to(SeqMap),
        wildcard.map(_.toOption.get),
        indexes,
        typeHint
      )
    }
  }

  final case class Checked(
    name: Name,
    alias: Option[Name],
    hasDocTTL: Boolean,
    defined: SeqMap[String, (SchemaTypeExpr, Boolean)],
    computed: SeqMap[String, TypeExpr],
    wildcard: Option[TypeExpr],
    indexes: SeqMap[String, Seq[(Path, Boolean)]],
    typeHint: TypeShape.TypeHint
  ) extends CollectionTypeInfo(
        name,
        hasDocTTL,
        defined.view.mapValues { case (te, d) => (Right(te), d) }.to(SeqMap),
        computed.view.mapValues(uncheckedType).to(SeqMap),
        wildcard.map(Right(_)),
        indexes,
        typeHint
      ) {

    lazy val aliased = alias.map(a => copy(name = a, alias = None))

    // Persistable typescheme of inferred computed field types
    lazy val computedFieldsExpr =
      Option.when(computed.nonEmpty) {
        val fields = computed.view.map { case (n, te) =>
          Name(n, Span.Null) -> te
        }.toSeq
        TypeExpr.Scheme(Seq.empty, TypeExpr.Object(fields, None, Span.Null))
      }
  }
}

sealed abstract class CollectionTypeInfo(
  name: Name,
  hasDocTTL: Boolean,
  defined: SeqMap[String, (Either[Type, SchemaTypeExpr], Boolean)],
  computed: SeqMap[String, Type],
  baseWildcard: Option[Either[Type, TypeExpr]],
  indexes: SeqMap[String, Seq[(Path, Boolean)]],
  typeHint: TypeShape.TypeHint
) {

  import CollectionTypeInfo.{ uncheckedType, MetaFields, RangeType }
  import Type._

  // convenience conversion to TypeScheme.Simple. Skips the runtime var check
  // of Type.typescheme
  private implicit def toTypeScheme(ty: Type): TypeScheme = TypeScheme.Simple(ty)

  // This duplicates logic from `Wildcard.expectedType`.
  private lazy val wildcard = if (baseWildcard.isEmpty && defined.isEmpty) {
    Some(Type.Any)
  } else {
    baseWildcard.map {
      // it's a poison var
      case Left(ty)  => ty
      case Right(te) => uncheckedType(te)
    }
  }

  val docName = name
  val nullDocName = name.copy(str = CollectionTypeInfo.nullDocName(name.str))
  val collName = name.copy(str = CollectionTypeInfo.collName(name.str))

  val docType = Type.Named(docName, name.span)
  val refType = Type.Ref(docType, docType.span)
  val emptyRefType = Type.EmptyRef(docType, docType.span)
  val nullDocType = Type.Named(nullDocName, name.span)
  val collType = Type.Named(collName, name.span)

  lazy val docImplType = {
    val definedTypes = defined.view.mapValues {
      // it's a poison var
      case (Left(ty), _)   => ty
      case (Right(ste), _) => uncheckedType(ste.asTypeExpr)
    }
    val fields = MetaFields ++ definedTypes ++ computed
    Record(fields.to(SeqMap), wildcard)
  }

  lazy val allShapes = Seq(
    docName.str -> docShape,
    nullDocName.str -> nullDocShape,
    collName.str -> collShape)

  lazy val docShape = TypeShape(
    self = docType,
    fields = docShapeFields,
    docType = CollectionTypeInfo.docTag(name.str),
    alias = Some(TypeScheme.Simple(docImplType))
  )

  lazy val nullDocShape = TypeShape(
    self = nullDocType,
    fields = nullShapeFields,
    docType = CollectionTypeInfo.nullDocTag(name.str),
    alias = Some(TypeScheme.Simple(Type.Null))
  )

  lazy val indexMethodTypes: SeqMap[String, Type] = indexes.view
    .map { case (name, terms) =>
      val tparams = terms.zipWithIndex.map { case ((path, mva), i) =>
        val ty = termType(selectFieldFromValue(docImplType, path), mva)
        // Start counting at 1 for term names
        Some(Name(s"term${i + 1}", Span.Null)) -> ty
      }

      val withoutRange = Function(
        tparams.to(ArraySeq),
        None,
        Set(docType),
        Span.Null
      )
      val withRange = Function(
        tparams.to(ArraySeq) :+ (Some(Name("range", Span.Null)) -> RangeType),
        None,
        Set(docType),
        Span.Null
      )

      name -> Intersect(withoutRange, withRange)
    }
    .to(SeqMap)

  lazy val collShape = {

    TypeShape(
      self = collType,
      fields = collShapeFields ++ indexMethodTypes.view.mapValues(TypeScheme.Simple),
      ops = Typer.HardcodedOps
        .filter { case (name, _) => ModelTyper.ComparisonOps.contains(name) }
        .map { case (name, ty) =>
          name -> ty.raw
            .asInstanceOf[Type.Function]
            .copy(params = ArraySeq(Some(Name("other", Span.Null)) -> Type.Any))
        },
      apply = Some(Function(("id" -> ID) -> Ref(docType))),
      typeHint = Some(typeHint)
    )
  }

  private def docShapeFields: Map[String, TypeScheme] = Map(
    "coll" -> collType,
    "update" -> Function(("data" -> updateType) -> docType),
    "replace" -> Function(("data" -> replaceType) -> docType),
    "updateData" -> Function(("data" -> updateDataType) -> docType),
    "replaceData" -> Function(("data" -> replaceDataType) -> docType),
    "exists" -> Function(() -> Singleton(Literal.True)),
    // FIXME: need to fix this in model
    "delete" -> Function(() -> nullDocType)
  )

  private def nullShapeFields: Map[String, TypeScheme] = Map(
    "id" -> ID,
    "coll" -> collType,
    "exists" -> Function(() -> Singleton(Literal.False)),
    "toString" -> Function(() -> Str)
  )

  private def collShapeFields: Map[String, TypeScheme] = Map(
    "byId" -> Function(("id" -> ID) -> refType),
    "create" -> Function(("data" -> createType) -> docType),
    "createData" -> Function(("data" -> createDataType) -> docType),
    "definition" -> Named("CollectionDef"),
    "all" -> Intersect(
      Function(() -> Set(docType)),
      Function(("range" -> RangeType) -> Set(docType))),
    "where" -> Function(("pred" -> Function(docType -> Boolean)) -> Set(docType)),
    "firstWhere" ->
      Function(("pred" -> Function(docType -> Boolean)) -> Optional(docType)),
    "toString" -> Function(() -> Str)
  )

  // The below types are a bit complex, but it effectively boils down to this:
  //
  // Function      |  Defined fields  | Allows `id` | Allows `ttl` | Allows `data`
  // create()      |    required      |      x      |      x       |
  // createData()  |    required      |             |              |       x
  // update()      |    optional      |             |      x       |
  // updateData()  |    optional      |             |              |       x
  // replace()     |    required      |             |      x       |
  // replaceData() |    required      |             |              |       x
  //
  // Only `createData` and `replaceData` have the same signature, so I figured it
  // would be clearer to just manually create a signature for every function.

  // This requires all defined fields, and allows the `id` and `ttl` fields.
  lazy val createType =
    Record(createNativeFields ++ createUserFields, wildcard = wildcard)

  // This requires all defined fields. `id` and `ttl` are disallowed.
  lazy val createDataType =
    Record(createUserFields.to(SeqMap), wildcard = wildcard)

  // This allows a partial set of fields, so they are all optional here. `id` is
  // disallowed and `ttl` is allowed.
  lazy val updateType =
    Record(updateNativeFields ++ updateUserFields, wildcard = wildcard)

  // This allows a partial set of fields. `id` and `ttl` are disallowed.
  lazy val updateDataType =
    Record(updateUserFields.to(SeqMap), wildcard = wildcard)

  // This requires all defined fields. `id` is disallowed and `ttl` is allowed.
  lazy val replaceType =
    Record(updateNativeFields ++ createUserFields, wildcard = wildcard)

  // This requires all defined fields. `id` and `ttl` are disallowed.
  lazy val replaceDataType =
    Record(createUserFields.to(SeqMap), wildcard = wildcard)

  // If a wildcard is defined, prohibits setting the `data` field by requiring it
  // to be null.
  private def nullDataIfWC =
    if (wildcard.nonEmpty) SeqMap("data" -> Null) else SeqMap.empty[String, Type]

  // Add the ttl field if in schema, or if there is a wildcard, disallow it by
  // requiring `null`.
  private def maybeTTL = if (hasDocTTL) {
    SeqMap("ttl" -> Optional(Time))
  } else if (wildcard.nonEmpty) {
    SeqMap("ttl" -> Null)
  } else {
    SeqMap.empty[String, Type]
  }

  private def createNativeFields =
    SeqMap("id" -> Optional(ID)) ++ maybeTTL ++ nullDataIfWC

  private def updateNativeFields =
    maybeTTL ++ nullDataIfWC

  private def createUserFields = {
    def go(te: SchemaTypeExpr, hasDefault: Boolean): Type = {
      te match {
        case SchemaTypeExpr.Object(fields, wildcard, span) =>
          val rec = Type.Record(
            fields.view
              .map { case (n, te, d) => n.str -> go(te, d.isDefined) }
              .to(SeqMap),
            wildcard.map(w => uncheckedType(upcastNumericParam(w))),
            span)

          // If all fields have defaults, then the parent is optional.
          if (hasDefault || fields.forall(_._3.isDefined)) {
            Type.Optional(rec)
          } else {
            rec
          }

        case SchemaTypeExpr.Simple(te) =>
          val ty = uncheckedType(upcastNumericParam(te))
          if (hasDefault) Type.Optional(ty) else ty
      }
    }

    defined.view.mapValues {
      // it's a poisoned var
      case (Left(ty), _)  => ty
      case (Right(te), d) => go(te, d)
    }
  }

  private def updateUserFields = {
    def go(te: SchemaTypeExpr): Type = {
      te match {
        case SchemaTypeExpr.Object(fields, wildcard, span) =>
          val rec = Type.Record(
            fields.view.map { case (n, te, _) => n.str -> go(te) }.to(SeqMap),
            wildcard.map(w => uncheckedType(upcastNumericParam(w))),
            span
          )

          Type.Optional(rec)

        case SchemaTypeExpr.Simple(te) =>
          Type.Optional(uncheckedType(upcastNumericParam(te)))
      }
    }

    defined.view.mapValues {
      // it's a poisoned var
      case (Left(ty), _)  => ty
      case (Right(te), _) => go(te)
    }
  }

  private def termType(ty: Type, mva: Boolean): Type = {
    if (!mva) return ty
    // T -> T
    // Array<T> -> T
    // Array<Array<T>> -> Array<T>
    // Array<T> | Array<U> -> T | U
    // Array<T> | U -> T | U
    def mvaTerm(ty: Type): Seq[Type] =
      ty match {
        case Type.Array(elem, _)  => Seq(elem)
        case Type.Tuple(elems, _) => elems
        case Type.Union(elems, _) => elems.flatMap(mvaTerm)
        case _                    => Seq(ty)
      }

    mvaTerm(ty) match {
      case Seq(ty) => ty
      case tys     => Type.Union(tys.to(ArraySeq), ty.span)
    }
  }

  private def selectFieldFromValue(ty: Type, path: Path): Type = {
    // Returns the type of `v[segment]`, or `None` if there isn't one.
    def select0(ty: Type, elem: PathElem): Option[Type] =
      (ty, elem) match {
        case (Type.Record(fields, wildcard, _), PathElem.Field(field, _)) =>
          fields.get(field).orElse(wildcard)

        case (Type.Tuple(elems, _), PathElem.Index(idx, _)) => elems.lift(idx.toInt)
        case (Type.Array(ty, _), PathElem.Index(_, _))      => Some(ty)

        case _ => None
      }

    @annotation.tailrec
    def go(ty: Type, path: List[PathElem]): Type =
      path.headOption match {
        case Some(elem) =>
          select0(ty, elem) match {
            case Some(ty) => go(ty, path.drop(1))
            // This will show up as a typechecking error somewhere, so we'll
            // just leave this as Any.
            case None => Type.Any
          }
        case None => ty
      }

    go(ty, path.elems)
  }

  private def upcastNumericParam(te: TypeExpr): TypeExpr = {
    def normalizeName(name: String) = name match {
      case Type.Int.name.str | Type.Long.name.str | Type.Double.name.str |
          Type.Float.name.str =>
        Type.Number.name.str
      case n => n
    }

    te match {
      case TypeExpr.Id(n, sp) => TypeExpr.Id(normalizeName(n), sp)
      case TypeExpr.Cons(Name(n, nsp), targs, sp) =>
        TypeExpr.Cons(Name(normalizeName(n), nsp), targs.map(upcastNumericParam), sp)

      case _: TypeExpr.Hole | _: TypeExpr.Any | _: TypeExpr.Never |
          _: TypeExpr.Singleton =>
        te

      case TypeExpr.Object(fs, w, sp) =>
        TypeExpr.Object(
          fs.map { case (n, te) => n -> upcastNumericParam(te) },
          w.map(upcastNumericParam),
          sp)
      case TypeExpr.Interface(fs, sp) =>
        TypeExpr.Interface(
          fs.map { case (n, te) => n -> upcastNumericParam(te) },
          sp)
      case TypeExpr.Projection(p, r, sp) =>
        TypeExpr.Projection(upcastNumericParam(p), upcastNumericParam(r), sp)
      case TypeExpr.Tuple(es, sp) =>
        TypeExpr.Tuple(es.map(upcastNumericParam), sp)
      case TypeExpr.Lambda(ps, v, r, sp) =>
        TypeExpr.Lambda(
          ps.map { case (n, te) => n -> upcastNumericParam(te) },
          v.map { case (n, te) => n -> upcastNumericParam(te) },
          upcastNumericParam(r),
          sp)
      case TypeExpr.Union(ms, sp) =>
        TypeExpr.Union(ms.map(upcastNumericParam), sp)
      case TypeExpr.Intersect(ms, sp) =>
        TypeExpr.Intersect(ms.map(upcastNumericParam), sp)
      case TypeExpr.Difference(e, s, sp) =>
        TypeExpr.Difference(upcastNumericParam(e), upcastNumericParam(s), sp)
      case TypeExpr.Recursive(n, in, sp) =>
        TypeExpr.Recursive(n, upcastNumericParam(in), sp)
      case TypeExpr.Nullable(b, qsp, sp) =>
        TypeExpr.Nullable(upcastNumericParam(b), qsp, sp)
    }
  }
}
