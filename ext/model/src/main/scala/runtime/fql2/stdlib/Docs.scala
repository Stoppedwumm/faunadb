package fauna.model.runtime.fql2.stdlib

import fauna.model.runtime.fql2._
import fauna.model.runtime.Effect
import fauna.model.schema.CollectionConfig
import fauna.repo.query.Query
import fauna.repo.schema.DataMode
import fauna.repo.values._
import fql.ast.Name
import fql.env.CollectionTypeInfo
import fql.typer.{ Type, TypeScheme, TypeShape }

object DocCompanion extends CompanionObject("Doc") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Doc => true
    case _            => false
  }
}

sealed abstract class AbstractNullDocPrototype
    extends Prototype(TypeTag.AnyNullDoc, isPersistable = true) {
  defMethod("exists" -> tt.False)() { (_, _) => sys.error("unreachable") }

  def typeShapeColl(coll: TypeScheme) = {
    val shape = typeShape
    shape.copy(fields = shape.fields ++ Map("coll" -> coll))
  }
}

object NullDocPrototype extends AbstractNullDocPrototype {
  defField("id" -> tt.ID) { (_, _) => sys.error("unreachable") }
}

object NamedNullDocPrototype extends AbstractNullDocPrototype {
  defField("name" -> tt.Str) { (_, _) => sys.error("unreachable") }
}

abstract class AbstractDocPrototype(
  val collName: String,
  val docType: TypeTag[Value.Doc],
  val nullDocType: TypeTag[Value.Doc])
    extends Prototype(docType, isPersistable = true)
    with DynamicFieldTable[Value.Doc] {
  import FieldTable.R

  lazy val docCreateType: Type = Type.AnyRecord
  lazy val docCreateDataType: Type = Type.AnyRecord
  lazy val docUpdateType: Type = Type.AnyRecord
  lazy val docUpdateDataType: Type = Type.AnyRecord
  lazy val docReplaceType: Type = Type.AnyRecord
  lazy val docReplaceDataType: Type = Type.AnyRecord
  lazy val docImplType: Type = Type.AnyRecord

  def this(collName: String) =
    this(collName, TypeTag.NamedDoc(collName), TypeTag.NullDoc(collName))

  override def getTypeShape =
    super.getTypeShape.copy(docType = TypeShape.DocType.Doc(collName))

  def dynHasField(ctx: FQLInterpCtx, self: Value.Doc, name: Name): Query[Boolean] =
    ReadBroker.hasField(ctx, self, name)

  def dynGet(ctx: FQLInterpCtx, self: Value.Doc, name: Name): Query[R[Value]] =
    ReadBroker.getField(ctx, self, name)

  private val NullDocMethods = Set("exists")

  private def guardNullDoc[A](
    ctx: FQLInterpCtx,
    self: Value.Doc,
    name: Name,
    get: => Query[R[A]]): Query[R[A]] =
    // if null docs also respond to the method, no need to check existence
    if (NullDocMethods contains name.str) {
      get
    } else {
      ReadBroker.guardFromNull(ctx, self, Effect.Action.Field).flatMap {
        case R.Val(_)   => get
        case n: R.Null  => n.toQuery
        case e: R.Error => e.toQuery
      }
    }

  override def getField(
    ctx: FQLInterpCtx,
    self: Value.Doc,
    name: Name): Query[R[Value]] =
    if (!hasResolver(name.str)) {
      // let the read broker handle the existence check.
      dynGet(ctx, self, name)
    } else {
      guardNullDoc(ctx, self, name, super.getField(ctx, self, name))
    }

  override def getMethod(
    ctx: FQLInterpCtx,
    self: Value.Doc,
    name: Name): Query[R[Option[NativeMethod[Value.Doc]]]] =
    guardNullDoc(ctx, self, name, super.getMethod(ctx, self, name))

  defAccess(tt.Any)("key" -> tt.Str) { (ctx, self, key, span) =>
    getField(ctx, self, Name(key.value, span)) map { _.toResult }
  }

  // The static type of doc.exists() is true, whereas the static type of
  // NullDoc.exists() is false. This lets us do things like:
  // ```
  // User.byId("1234")!.exists() // types to 'true'
  // ```
  defMethod("exists" -> tt.True)() { (ctx, self) =>
    ReadBroker.guardFromNull(ctx, self, Effect.Action.Function("exists")).map {
      case R.Null(_)  => Result.Ok(Value.False)
      case R.Val(_)   => Result.Ok(Value.True)
      case R.Error(e) => Result.Err(e)
    }
  }

  defMethod("update" -> docType)("data" -> tt.Struct(docUpdateType)) {
    (ctx, self, data) =>
      WriteBroker.updateDocument(ctx, self, DataMode.Default, data)
  }

  defMethod("replace" -> docType)("data" -> tt.Struct(docReplaceType)) {
    (ctx, self, data) =>
      WriteBroker.replaceDocument(ctx, self, DataMode.Default, data)
  }

  defMethod("delete" -> nullDocType)() { (ctx, self) =>
    WriteBroker.deleteDocument(ctx, self)
  }
}

object NativeDocPrototype {
  def apply(collName: String) =
    new NativeDocPrototype(
      collName,
      TypeTag.NamedDoc(collName),
      TypeTag.NullDoc(collName))

  /** This is only a valid prototype at runtime. It has the static type of a collection
    * with an empty string, so it should never be used for a static type.
    */
  val Any = NativeDocPrototype("")
}

final case class NativeDocPrototype(
  override val collName: String,
  override val docType: TypeTag[Value.Doc],
  override val nullDocType: TypeTag[Value.Doc])
    extends AbstractDocPrototype(collName, docType, nullDocType)

object UserDocPrototype {
  def apply(coll: CollectionConfig, collName: String) =
    new UserDocPrototype(
      coll,
      collName,
      TypeTag.NamedDoc(collName),
      TypeTag.NullDoc(collName))

  /** This is only a valid prototype at runtime. It has the static type of a collection
    * with an empty string, so it should never be used for a static type.
    */
  val Any = RuntimeUserDocPrototype("", TypeTag.NamedDoc(""), TypeTag.NullDoc(""))
}

final case class RuntimeUserDocPrototype(
  override val collName: String,
  override val docType: TypeTag[Value.Doc],
  override val nullDocType: TypeTag[Value.Doc])
    extends AbstractDocPrototype(collName, docType, nullDocType) {

  // NB: This is the runtime prototype! These don't have the right static signatures.
  defMethod("updateData" -> docType)("data" -> tt.AnyStruct) { (ctx, self, data) =>
    WriteBroker.updateDocument(ctx, self, DataMode.PlainData, data)
  }

  defMethod("replaceData" -> docType)("data" -> tt.AnyStruct) { (ctx, self, data) =>
    WriteBroker.replaceDocument(ctx, self, DataMode.PlainData, data)
  }
}

final case class UserDocPrototype(
  coll: CollectionConfig,
  override val collName: String,
  override val docType: TypeTag[Value.Doc],
  override val nullDocType: TypeTag[Value.Doc])
    extends AbstractDocPrototype(collName, docType, nullDocType) {

  private lazy val typeInfo: CollectionTypeInfo = coll.typeInfo

  override lazy val docCreateType = typeInfo.createType
  override lazy val docCreateDataType = typeInfo.createDataType
  override lazy val docUpdateType = typeInfo.updateType
  override lazy val docUpdateDataType = typeInfo.updateDataType
  override lazy val docReplaceType = typeInfo.replaceType
  override lazy val docReplaceDataType = typeInfo.replaceDataType
  override lazy val docImplType = typeInfo.docImplType

  defMethod("updateData" -> docType)("data" -> tt.Struct(docUpdateDataType)) {
    (ctx, self, data) =>
      WriteBroker.updateDocument(ctx, self, DataMode.PlainData, data)
  }

  defMethod("replaceData" -> docType)("data" -> tt.Struct(docReplaceDataType)) {
    (ctx, self, data) =>
      WriteBroker.replaceDocument(ctx, self, DataMode.PlainData, data)
  }
}
