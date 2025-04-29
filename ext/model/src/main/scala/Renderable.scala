package fauna.model

import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.codex.json.JSValue
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.runtime.Effect
import fauna.model.schema.{ NamedCollectionID, NativeCollectionID }
import fauna.model.RefParser.RefScope
import fauna.model.RenderContext._
import fauna.model.SchemaNames.PendingNames
import fauna.model.Transformations._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index.IndexTerm
import fauna.storage.ir.QueryV
import fauna.util.Base64
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.time._
import java.util.UUID
import scala.annotation.tailrec
import scala.util.control.NoStackTrace

object RenderContext {

  def apply(
    auth: Auth,
    version: APIVersion,
    transactionTS: Timestamp): Query[RenderContext] =
    SchemaNames.modifiedNamedCollections map { pending =>
      RenderContext(auth.scopeID, version, transactionTS, pending)
    }

  def renderTo(
    auth: Auth,
    version: APIVersion,
    txnTS: Timestamp,
    pending: PendingNames,
    buf: ByteBuf,
    r: Literal,
    pretty: Boolean): Query[ByteBuf] =
    RenderContext(auth.scopeID, version, txnTS, pending).renderTo(buf, r, pretty)

  def render(
    auth: Auth,
    version: APIVersion,
    txnTS: Timestamp,
    r: Literal,
    pretty: Boolean = false): Query[ByteBuf] =
    apply(auth, version, txnTS) flatMap { _.render(r, pretty) }

  final case class CacheMiss(scope: ScopeID, id: DocID)
      extends Exception
      with NoStackTrace

  val RefTag = JSON.Escaped("@ref")
  val SetRefTag = JSON.Escaped("@set")
  val TimeTag = JSON.Escaped("@ts")
  val DateTag = JSON.Escaped("@date")
  val BytesTag = JSON.Escaped("@bytes")
  val UUIDTag = JSON.Escaped("@uuid")
  val QueryTag = JSON.Escaped("@query")
  val ObjectTag = JSON.Escaped("@obj")

  final object Escaped {
    val ID = JSON.Escaped("id")
    val Database = JSON.Escaped("database")
    val Ref = JSON.Escaped("ref")
    val TS = JSON.Escaped("ts")
    val Class = JSON.Escaped("class")
    val Collection = JSON.Escaped("collection")
    val Before = JSON.Escaped("before")
    val After = JSON.Escaped("after")
    val Data = JSON.Escaped("data")
    val Value = JSON.Escaped("value")
    val Values = JSON.Escaped("values")
    val Sources = JSON.Escaped("sources")
    val Action = JSON.Escaped("action")
    val Resource = JSON.Escaped("resource")
    val Instance = JSON.Escaped("instance")
    val Document = JSON.Escaped("document")

    val Events = JSON.Escaped("events")
    val Singleton = JSON.Escaped("singleton")
    val Documents = JSON.Escaped("documents")
    val Create = JSON.Escaped("create")
    val Update = JSON.Escaped("update")
    val Delete = JSON.Escaped("delete")
    val Add = JSON.Escaped("add")
    val Remove = JSON.Escaped("remove")

    val Match = JSON.Escaped("match")
    val Terms = JSON.Escaped("terms")
    val Union = JSON.Escaped("union")
    val Intersection = JSON.Escaped("intersection")
    val Difference = JSON.Escaped("difference")
    val Distinct = JSON.Escaped("distinct")
    val Join = JSON.Escaped("join")
    val With = JSON.Escaped("with")
    val Owner = JSON.Escaped("owner")
    val Member = JSON.Escaped("member")
    val Filter = JSON.Escaped("filter")
    val Range = JSON.Escaped("range")
    val From = JSON.Escaped("from")
    val To = JSON.Escaped("to")
    val Reverse = JSON.Escaped("reverse")

    def getCollection(version: APIVersion): JSON.Escaped = {
      if (version >= APIVersion.V27) {
        Escaped.Collection
      } else {
        Escaped.Class
      }
    }

    def getDocument(version: APIVersion): JSON.Escaped = {
      if (version >= APIVersion.V27) {
        Escaped.Document
      } else {
        Escaped.Instance
      }
    }
  }

  val ObjectEscapeStartBytes = "{\"@obj\":".toUTF8Bytes
  val ObjectEscapeEndByte = '}'.toByte
  val AtSignByte = '@'.toByte
  val SlashByte = '/'.toByte

  sealed trait RenderedRef
  case class UndefinedRef(scope: ScopeID, id: DocID) extends RenderedRef
  case class DefinedRef(
    id: Either[String, SubID],
    cls: Option[DefinedRef],
    db: Option[DefinedRef])
      extends RenderedRef {

    def legacyString: String = {
      val idString = id match {
        case Left(str)    => str
        case Right(subID) => subID.toLong.toString
      }

      cls match {
        case Some(clsRef) => s"${clsRef.legacyString}/$idString"
        case None         => idString
      }
    }
  }

  object RenderedRef {
    object DatabaseCollection extends DefinedRef(Left("databases"), None, None)
    object IndexCollection extends DefinedRef(Left("indexes"), None, None)
    object ClassCollection extends DefinedRef(Left("classes"), None, None)
    object CollectionCollection extends DefinedRef(Left("collections"), None, None)
    object KeyCollection extends DefinedRef(Left("keys"), None, None)
    object TokenCollection extends DefinedRef(Left("tokens"), None, None)
    object CredentialsCollection extends DefinedRef(Left("credentials"), None, None)
    object TaskCollection extends DefinedRef(Left("tasks"), None, None)
    object UserFunctionCollection extends DefinedRef(Left("functions"), None, None)
    object RoleCollection extends DefinedRef(Left("roles"), None, None)
    object AccessProviderCollection extends DefinedRef(Left("access_providers"), None, None)
  }

  case class VersionedRenderedRef(version: APIVersion) {

    // necessary for rendering UnresolvedRefError
    import RefParser.RefScope._
    import RenderedRef._

    def render(ref: Ref): DefinedRef =
      ref match {
        case DatabaseClassRef        => DatabaseCollection
        case CollectionCollectionRef => ClassCollection
        case IndexClassRef           => IndexCollection
        case KeyClassRef             => KeyCollection
        case TokenClassRef           => TokenCollection
        case CredentialsClassRef     => CredentialsCollection
        case UserFunctionClassRef    => UserFunctionCollection
        case RoleClassRef            => RoleCollection
        case AccessProviderClassRef  => AccessProviderCollection

        case NativeCollectionRef(id, scope) =>
          NativeCollectionID.toName(id, version) match {
            case Some(name) => DefinedRef(Left(name), None, scope map render)
            case None =>
              throw new IllegalArgumentException(
                s"Unknown native collection ref: $id")
          }

        case DatabaseRef(name, scope) =>
          DefinedRef(Left(name), Some(DatabaseCollection), scope map render)
        case UserCollectionRef(name, scope) =>
          if (version >= APIVersion.V27) {
            DefinedRef(Left(name), Some(CollectionCollection), scope map render)
          } else {
            DefinedRef(Left(name), Some(ClassCollection), scope map render)
          }
        case IndexRef(name, scope) =>
          DefinedRef(Left(name), Some(IndexCollection), scope map render)
        case UserFunctionRef(name, scope) =>
          DefinedRef(Left(name), Some(UserFunctionCollection), scope map render)
        case RoleRef(name, scope) =>
          DefinedRef(Left(name), Some(RoleCollection), scope map render)
        case KeyRef(id, scope) =>
          DefinedRef(Right(SubID(id.toLong)), Some(KeyCollection), scope map render)

        case AccessProviderRef(name, scope) =>
          DefinedRef(Left(name), Some(AccessProviderCollection), scope map render)

        case SelfRef(cls)         => DefinedRef(Left("self"), Some(render(cls)), None)
        case InstanceRef(id, cls) => DefinedRef(id, Some(render(cls)), None)
      }
  }
}

sealed abstract class AbstractRenderContext(
  reqScope: ScopeID,
  version: APIVersion,
  transactionTS: Timestamp,
  pendingNames: PendingNames) {

  protected def useTags: Boolean

  protected def writeSetRef(out: JSON.Out, id: EventSet): Unit

  @volatile private[this] var lookups = Map.empty[(ScopeID, DocID), RenderedRef]

  final def renderTo(
    buf: ByteBuf,
    result: Literal,
    pretty: Boolean): Query[ByteBuf] =
    if (pretty) {
      render(result, false) map { JSON.parse[JSValue](_).writeTo(buf, true) }
    } else {
      val out = JSONWriter(buf)
      val start = out.marker

      def loop(): Query[ByteBuf] =
        try {
          out.orWriteNull(LiteralEncoder.encode(out, result))
          Query.value(buf)
        } catch {
          case e: CacheMiss =>
            out.reset(start)
            populateCacheMiss(e) flatMap { _ =>
              loop()
            }
        }

      loop()
    }

  final def render(result: Literal, pretty: Boolean = false): Query[ByteBuf] =
    renderTo(Unpooled.buffer, result, pretty)

  final protected def populateCacheMiss(miss: CacheMiss): Query[RenderedRef] =
    miss.id match {
      // Native collections don't have in disk representation. Skip lookup.
      case CollectionID(NativeCollectionID(_)) =>
        mkRef(miss.scope, miss.id)
      case _ =>
        SchemaNames.lookupCachedName(miss.scope, miss.id, pendingNames) flatMap { _ =>
          mkRef(miss.scope, miss.id)
        }
    }

  private def scopeRef(
    scope: ScopeID,
    clsID: CollectionID): Query[Option[RenderedRef]] =
    if (scope == reqScope || scope == Database.RootScopeID) {
      Query.none
    } else {
      Query(NativeCollectionID.toName(clsID, version)) flatMapT { _ =>
        Database.forScope(scope) flatMap {
          case None => Query.some(UndefinedRef(scope, clsID.toDocID))
          case Some(db) =>
            mkRef(db.parentScopeID, db.id.toDocID) map { Some(_) }
        }
      }
    }

  private def nativeRenderedRef(id: CollectionID): RenderedRef =
    id match {
      case NativeCollectionID.Database => RenderedRef.DatabaseCollection
      case NativeCollectionID.Index    => RenderedRef.IndexCollection
      case NativeCollectionID.Collection =>
        if (version < APIVersion.V27) {
          RenderedRef.ClassCollection
        } else {
          RenderedRef.CollectionCollection
        }
      case NativeCollectionID.Key            => RenderedRef.KeyCollection
      case NativeCollectionID.Token          => RenderedRef.TokenCollection
      case NativeCollectionID.Credentials    => RenderedRef.CredentialsCollection
      case NativeCollectionID.Task           => RenderedRef.TaskCollection
      case NativeCollectionID.UserFunction   => RenderedRef.UserFunctionCollection
      case NativeCollectionID.Role           => RenderedRef.RoleCollection
      case NativeCollectionID.AccessProvider => RenderedRef.AccessProviderCollection
      case _ =>
        throw new IllegalArgumentException(s"$id must be a native collection")
    }

  private def mkRef(scope: ScopeID, id: DocID): Query[RenderedRef] =
    id match {
      case CollectionID(NativeCollectionID(native))
          if scope == reqScope || scope == Database.RootScopeID =>
        Query.value(nativeRenderedRef(native.collID))

      case CollectionID(NativeCollectionID(native)) =>
        lookups.get((scope, id)) match {
          case Some(ref) => Query.value(ref)
          case None =>
            val dbRef = scopeRef(scope, native.collID)
            val clsRef = Query.value(nativeRenderedRef(native.collID))

            val res = (dbRef, clsRef) par {
              case (Some(db: DefinedRef), DefinedRef(Left(cls), _, _)) =>
                Query.value(DefinedRef(Left(cls), None, Some(db)))

              case (None, DefinedRef(Left(cls), _, _)) =>
                Query.value(DefinedRef(Left(cls), None, None))

              case (_, _) =>
                Query.value(UndefinedRef(scope, id))
            }

            res map { ref =>
              synchronized { lookups += ((scope, id) -> ref) }
              ref
            }
        }

      case DocID(subID, clsID) =>
        lookups.get((scope, id)) match {
          case Some(ref) => Query.value(ref)
          case None =>
            val dbRef = scopeRef(scope, clsID)
            val clsRef = Query.some(nativeRenderedRef(clsID))

            val idVal: Query[Either[String, SubID]] =
              id match {
                case DocID(SubID(0), DatabaseID.collID) =>
                  Query.value(Left("__root__"))

                case DocID(_, NamedCollectionID(_)) =>
                  SchemaNames.lookupCachedName(scope, id, pendingNames) map {
                    case Some(name) => Left(name)
                    case None       => null
                  }
                case _ =>
                  Query.value(Right(subID))
              }

            val res = (dbRef, clsRef, idVal) par {
              case (Some(UndefinedRef(_, _)), _, _) =>
                Query.value(UndefinedRef(scope, id))

              case (_, Some(UndefinedRef(_, _)), _) =>
                Query.value(UndefinedRef(scope, id))

              case (dbOpt, clsOpt, idVal) if idVal ne null =>
                val db = dbOpt.asInstanceOf[Option[DefinedRef]]
                val cls = clsOpt.asInstanceOf[Option[DefinedRef]]
                Query.value(DefinedRef(idVal, cls, db))

              case (_, _, _) =>
                Query.value(UndefinedRef(scope, id))
            }

            res map { ref =>
              synchronized { lookups += ((scope, id) -> ref) }
              ref
            }
        }
    }

  // shared helpers

  final protected def renderedRef(scope: ScopeID, id: DocID): RenderedRef =
    id match {
      case CollectionID(NativeCollectionID(native))
          if scope == reqScope || scope == Database.RootScopeID =>
        nativeRenderedRef(native.collID)
      case CollectionID(NativeCollectionID(_)) =>
        lookups.get((scope, id)) match {
          case Some(rv) => rv
          case None     => throw CacheMiss(scope, id)
        }
      case DocID(_, NamedCollectionID(_)) =>
        lookups.get((scope, id)) match {
          case Some(rv) => rv
          case None     => throw CacheMiss(scope, id)
        }
      case DocID(sub, cls) =>
        renderedRef(scope, cls.toDocID) match {
          case ref: UndefinedRef => ref
          case ref: DefinedRef   => DefinedRef(Right(sub), Some(ref), None)
        }
    }

  final protected def writeIndexValue(
    out: JSON.Out,
    scope: ScopeID,
    t: IndexTerm): Unit =
    out.orWriteNull(LiteralEncoder.encode(out, Literal.fromIndexTerm(scope, t)))

  final protected def writeIndexValues(
    out: JSON.Out,
    scope: ScopeID,
    ts: Seq[IndexTerm]): Unit =
      ts match {
        case Seq(t) => writeIndexValue(out, scope, t)
        case ts =>
          out.writeArrayStart()
          ts foreach { writeIndexValue(out, scope, _) }
          out.writeArrayEnd()
      }

  final def writeTagged(out: JSON.Out, tag: JSON.Escaped, thunk: => Unit) = {
    if (useTags) {
      out.writeObjectStart()
      out.writeObjectField(tag, thunk)
      out.writeObjectEnd()
    } else {
      thunk
    }
  }

  final def writeTime(out: JSON.Out, ts: Timestamp) =
    writeTagged(out, TimeTag, out.writeString(ts.toString))

  final def writeDate(out: JSON.Out, date: LocalDate) =
    writeTagged(out, DateTag, out.writeString(date.toString))

  final def writeBytes(out: JSON.Out, bytes: ByteBuf) = {
    val byteStr = Base64.encodeStandardAscii(bytes.nioBuffer)
    writeTagged(out, BytesTag, out.writeString(byteStr))
  }

  final def writeUUID(out: JSON.Out, id: UUID) =
    writeTagged(out, UUIDTag, out.writeString(id.toString))

  final def writeRef(out: JSON.Out, ref: RenderedRef): Unit =
    ref match {
      case UndefinedRef(scope, id) =>
        throw new IllegalArgumentException(
          s"Cannot render undefined ref for $scope, $id")
      case DefinedRef(Left("__root__"), Some(RenderedRef.DatabaseCollection), None)
          if reqScope != Database.RootScopeID =>
        ()
      case ref @ DefinedRef(id, cls, db) =>
        if (version <= APIVersion.V20) {
          writeTagged(out, RefTag, out.writeString(ref.legacyString))
        } else {
          writeTagged(
            out,
            RefTag, {
              out.writeObjectStart()
              id match {
                case Left(str) =>
                  out.writeObjectField(Escaped.ID, out.writeString(str))
                case Right(subID) =>
                  out.writeObjectField(Escaped.ID,
                                       out.writeString(subID.toLong.toString))
              }
              cls foreach { ref =>
                out.writeNonNullObjectField(Escaped.getCollection(version),
                                            writeRef(out, ref))
              }
              db foreach { ref =>
                out.writeNonNullObjectField(Escaped.Database, writeRef(out, ref))
              }
              out.writeObjectEnd()
            }
          )
        }
    }

  final def writeUntaggedObject(out: JSON.Out)(thunk: => Any) = {
    out.writeObjectStart()
    thunk
    out.writeObjectEnd()
  }

  final def writeTaggedObject(out: JSON.Out, name: JSON.Escaped)(thunk: => Any) = {
    out.writeObjectStart()
    out.writeObjectField(name, thunk)
    out.writeObjectEnd()
  }

  final def writeArray(out: JSON.Out, sets: Seq[EventSet]): Unit = {
    if (sets.size == 1) {
      writeSetRef(out, sets.head)
    } else {
      out.writeArrayStart()
      sets foreach { writeSetRef(out, _) }
      out.writeArrayEnd()
    }
  }

  implicit final protected object LiteralEncoder extends JSON.Encoder[Literal] {

    def encode(out: JSON.Out, r: Literal) =
      r match {
        case LongL(l)               => out.writeNumber(l)
        case DoubleL(d)             => out.writeNumber(d)
        case TrueL                  => out.writeBoolean(true)
        case FalseL                 => out.writeBoolean(false)
        case NullL                  => out.writeNull()
        case StringL(str)           => out.writeString(str)
        case BytesL(b)              => writeBytes(out, b); out
        case TimeL(ts)              => writeTime(out, ts); out
        case DateL(date)            => writeDate(out, date); out
        case UUIDL(uuid)            => writeUUID(out, uuid); out
        case TransactionTimeL       => writeTime(out, transactionTS); out
        case TransactionTimeMicrosL => out.writeNumber(transactionTS.micros)
        case UnresolvedRefL(orig) =>
          writeRef(out, VersionedRenderedRef(version).render(orig)); out

        case SetL(set) => writeSetRef(out, set); out

        case l: LambdaL =>
          writeTagged(out, QueryTag, encode(out, l.renderableLiteral(version))); out

        case RefL(scope, id) =>
          renderedRef(scope, id) match {
            case ref: DefinedRef => writeRef(out, ref)
            case _               => ()
          }
          out

        case ArrayL(elems) =>
          out.writeArrayStart()
          val iter = elems.iterator
          while (iter.hasNext) encode(out, iter.next())
          out.writeArrayEnd()

        case ObjectL(elems) =>
          out.writeObjectStart()

          // save the index of the start of the object
          val startIdx = out.buf.writerIndex - 1
          var needsEscape = false
          val iter = elems.iterator

          while (iter.hasNext) {
            val (k, v) = iter.next()
            if (k startsWith "@") needsEscape = true
            out.writeNonNullObjectField(k.toUTF8Buf, encode(out, v))
          }
          out.writeObjectEnd()

          if (useTags && needsEscape) {
            out.buf.insertBytes(startIdx, ObjectEscapeStartBytes)
            out.buf.writeByte(ObjectEscapeEndByte)
          }

          out

        case VersionL(vers, _) =>
          out.writeObjectStart()

          renderedRef(vers.parentScopeID, vers.id) match {
            case ref: DefinedRef =>
              out.writeNonNullObjectField(Escaped.Ref, writeRef(out, ref))
            case _ => ()
          }
          if (version <= APIVersion.V20) {
            out.writeNonNullObjectField(
              Escaped.getCollection(version),
              writeRef(out, renderedRef(vers.parentScopeID, vers.collID.toDocID)))
          }
          val ts = vers.ts.resolve(transactionTS).validTS
          out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))

          if (vers.action.isDelete) {
            out.writeObjectField(Escaped.Action, out.writeString(Escaped.Delete))
          }

          val modifiedData =
            transformToVersion(
              version,
              AllAPITransformations,
              vers)

          val modifiedVers = vers match {
            case live: Version.Live       => live.withData(Data(modifiedData))
            case deleted: Version.Deleted => deleted
          }

          val data = ReadAdaptor.readableDataStream(modifiedVers)
          val length = data.read(DataSwitch.VersionDataStartSwitch)
          val switch = RenderDataSwitch(vers.parentScopeID, out)

          var i = 0
          while (i < length) {
            out.writeNonNullObjectField(data.read(DataSwitch.StringSwitch),
                                        data.read(switch))
            i += 1
          }

          out.writeObjectEnd()

        case PageL(elems, _, before, after) =>
          out.writeObjectStart()

          before foreach { v =>
            out.writeObjectField(Escaped.Before, encode(out, v))
          }
          after foreach { v =>
            out.writeObjectField(Escaped.After, encode(out, v))
          }

          out.writeObjectField(Escaped.Data, {
            out.writeArrayStart()
            elems foreach { encode(out, _) }
            out.writeArrayEnd()
          })

          out.writeObjectEnd()

        case ElemL(value, sources) =>
          val mk1 = out.marker

          out.writeObjectStart()
          out.writeString(Escaped.Value)
          val mk2 = out.marker
          encode(out, value)

          if (!out.isChanged(mk2)) {
            out.reset(mk1)
          } else {
            out.writeObjectField(Escaped.Sources, {
              out.writeArrayStart()
              sources foreach { writeSetRef(out, _) }
              out.writeArrayEnd()
            })
            out.writeObjectEnd()
          }

          out

        case CursorL(Left(evt: SetEventL)) =>
          encode(out, evt)

        case CursorL(Left(DocEventL(ev))) =>
          renderedRef(ev.scopeID, ev.docID) match {
            case ref: DefinedRef if version >= APIVersion.V21 =>
              out.writeObjectStart()
              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))
              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.getDocument(version), writeRef(out, ref))
              out.writeObjectEnd()

            case ref: DefinedRef =>
              out.writeObjectStart()
              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))
              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.Resource, writeRef(out, ref))
              out.writeObjectEnd()

            case _ => ()
          }

          out

        case CursorL(Right(arr)) =>
          encode(out, arr)

        case ActionL(action) if version >= APIVersion.V21 =>
          action match {
            case Remove => out.writeString(Escaped.Remove)
            case Delete => out.writeString(Escaped.Delete)
            case Update => out.writeString(Escaped.Update)
            case Create => out.writeString(Escaped.Create)
            case Add    => out.writeString(Escaped.Add)
          }

        case ActionL(action) =>
          val str = if (action.isCreate) Escaped.Create else Escaped.Delete
          out.writeString(str)

        case DocEventL(ev) =>
          renderedRef(ev.scopeID, ev.docID) match {
            case ref: DefinedRef if version >= APIVersion.V21 =>
              out.writeObjectStart()

              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))

              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.getDocument(version), writeRef(out, ref))

              out.writeObjectField(
                Escaped.Data, {
                  val diff = ReadAdaptor.readableDiffStream(ev)
                  val length = diff.read(DataSwitch.VersionDataStartSwitch)
                  if (length > 0) {
                    val switch = RenderDataSwitch(ev.scopeID, out)

                    var i = 0
                    while (i < length) {
                      val field = diff
                        .read(DataSwitch.StringSwitch)
                        .toUTF8String

                      if (field == "data") {
                        diff.read(switch)
                      } else {
                        diff.read(DataSwitch.SkipSwitch)
                      }
                      i += 1
                    }
                  } else {
                    out.writeNull()
                  }
                }
              )

              out.writeObjectEnd()

            case ref: DefinedRef =>
              out.writeObjectStart()

              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))

              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.Resource, writeRef(out, ref))
              out.writeObjectEnd()

            case _ => ()
          }

          out

        case SetEventL(ev) =>
          renderedRef(ev.scopeID, ev.docID) match {
            case ref: DefinedRef if version >= APIVersion.V21 =>
              out.writeObjectStart()

              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))

              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.getDocument(version), writeRef(out, ref))

              if (ev.values.nonEmpty) {
                out.writeObjectField(Escaped.Data, {
                  out.writeArrayStart()
                  ev.values foreach { writeIndexValue(out, ev.scopeID, _) }
                  out.writeArrayEnd()
                })
              }
              out.writeObjectEnd()

            case ref: DefinedRef =>
              out.writeObjectStart()

              val ts = ev.ts.resolve(transactionTS).validTS
              out.writeObjectField(Escaped.TS, out.writeNumber(ts.micros))

              out.writeObjectField(Escaped.Action, encode(out, ActionL(ev.action)))
              out.writeNonNullObjectField(Escaped.Resource, writeRef(out, ref))

              if (ev.values.nonEmpty) {
                out.writeObjectField(Escaped.Values, {
                  out.writeArrayStart()
                  ev.values foreach { writeIndexValue(out, ev.scopeID, _) }
                  out.writeArrayEnd()
                })
              }

              out.writeObjectEnd()
            case _ => ()
          }

          out
      }
  }

  /**
    * ** Mutation **
    * Provides functions that use the mutable `out` JSON builder to write out
    * values parsed from a stateful DataStream. Progresses the DataStream
    * using `read` when necessary.
    */
  private case class RenderDataSwitch(scope: ScopeID, out: JSON.Out)
      extends DataSwitch[Unit] {
    def readLong(l: Long, stream: DataStream) = out.writeNumber(l)
    def readDouble(d: Double, stream: DataStream) = out.writeNumber(d)
    def readBoolean(b: Boolean, stream: DataStream) = out.writeBoolean(b)
    def readNull(stream: DataStream) = out.writeNull()
    def readString(s: ByteBuf, stream: DataStream) = out.writeString(s)
    def readBytes(b: ByteBuf, stream: DataStream) = writeBytes(out, b)
    def readTime(ts: Timestamp, stream: DataStream) = writeTime(out, ts)
    def readDate(d: LocalDate, stream: DataStream) = writeDate(out, d)
    def readUUID(id: UUID, stream: DataStream) = writeUUID(out, id)

    def readDocID(id: DocID, stream: DataStream) =
      writeRef(out, renderedRef(scope, id))

    def readQuery(query: QueryV, stream: DataStream) =
      LiteralEncoder.encode(out, Literal(scope, query))

    def readTransactionTime(isMicros: Boolean, stream: DataStream) =
      if (isMicros) {
        out.writeNumber(transactionTS.micros)
      } else {
        writeTime(out, transactionTS)
      }

    def readArrayStart(length: Long, stream: DataStream) = {
      out.writeArrayStart()

      var i = 0
      while (i < length) {
        stream.read(this)
        i += 1
      }

      out.writeArrayEnd()
    }

    def readObjectStart(length: Long, stream: DataStream) = {
      out.writeObjectStart()

      // save the index of the start of the object
      val startIdx = out.marker.index - 1
      var needsEscape = false

      var i = 0
      while (i < length) {
        val k = stream.read(DataSwitch.StringSwitch)
        if (k.isReadable && k.getByte(k.readerIndex) == AtSignByte)
          needsEscape = true
        out.writeNonNullObjectField(k, stream.read(this))
        i += 1
      }

      out.writeObjectEnd()

      if (useTags && needsEscape) {
        out.buf.insertBytes(startIdx, ObjectEscapeStartBytes)
        out.buf.writeByte(ObjectEscapeEndByte)
      }
    }
  }
}

final case class RenderContext(
  reqScope: ScopeID,
  version: APIVersion,
  transactionTS: Timestamp,
  pendingNames: PendingNames)
    extends AbstractRenderContext(reqScope,
                                  version,
                                  transactionTS,
                                  pendingNames) {

  def useTags = true

  protected def writeSetRef(out: JSON.Out, set: EventSet): Unit = {
    def writeSetContents(set: EventSet): Unit =
      set match {
        case SchemaSet(scope, id) => writeRef(out, renderedRef(scope, id))
        case DocSet(scope, id)    => writeRef(out, renderedRef(scope, id))

        case HistoricalSet(set) =>
          writeTaggedObject(out, Escaped.Events) { writeSetContents(set) }

        case DocSingletonSet(scope, id) =>
          writeTaggedObject(out, Escaped.Singleton) {
            writeRef(out, renderedRef(scope, id))
          }

        case DocumentsSet(scope, id, _) =>
          writeTaggedObject(out, Escaped.Documents) {
            writeRef(out, renderedRef(scope, id.toDocID))
          }

        case IndexSet(idx, terms, _) =>
          renderedRef(idx.scopeID, idx.id.toDocID) match {
            case ref: DefinedRef =>
              writeTaggedObject(out, Escaped.Match) {
                writeRef(out, ref)

                if (terms.nonEmpty) {
                  out.writeObjectField(Escaped.Terms, writeIndexValues(out, idx.scopeID, terms))
                }
              }

            //TODO: An undefined ref will silently fall through here; is this what we want?
            case _ => ()
          }

        case Union(sets) =>
          writeTaggedObject(out, Escaped.Union) { writeArray(out, sets) }
        case Intersection(sets) =>
          writeTaggedObject(out, Escaped.Intersection) { writeArray(out, sets) }
        case Difference(sets) =>
          writeTaggedObject(out, Escaped.Difference) { writeArray(out, sets) }
        case Distinct(set) =>
          writeTaggedObject(out, Escaped.Distinct) { writeSetRef(out, set) }

        case RangeSet(set, from, to) =>
          writeUntaggedObject(out) {
            out.writeObjectField(Escaped.Range, writeSetRef(out, set))

            val fromTerms = from.terms

            if (from.terms.nonEmpty) {
              out.writeObjectField(Escaped.From, writeIndexValues(out, from.scopeID, fromTerms))
            }

            val toTerms = to.terms

            if (to.terms.nonEmpty) {
              out.writeObjectField(Escaped.To, writeIndexValues(out, to.scopeID, toTerms))
            }
          }

        case ReverseSet(set) =>
          writeUntaggedObject(out) {
            out.writeObjectField(Escaped.Reverse, writeSetRef(out, set))
          }

        case LambdaFilterSet(src, lambda, _) =>
          writeUntaggedObject(out) {
            out.writeObjectField(Escaped.Filter,
                                 LiteralEncoder.encode(out, lambda.renderableLiteral(version)))
            out.writeObjectField(Escaped.Collection, writeSetRef(out, src))
          }

        case LambdaJoin(src, lambda, _) =>
          writeUntaggedObject(out) {
            out.writeObjectField(Escaped.Join, writeSetRef(out, src))
            out.writeObjectField(Escaped.With,
                                 LiteralEncoder.encode(out, lambda.renderableLiteral(version)))
          }

        case IndexJoin(src, idx) =>
          writeUntaggedObject(out) {
            out.writeObjectField(Escaped.Join, writeSetRef(out, src))
            out.writeObjectField(
              Escaped.With,
              writeRef(out, renderedRef(idx.scopeID, idx.id.toDocID)))
          }

        case set => throw new IllegalArgumentException(s"Unknown set $set.")
      }

    set match {
      // Special cases: certain sets are rendered simply as a ref.
      case ss: SchemaSet => writeSetContents(ss)
      case is: DocSet    => writeSetContents(is)

      // In the typical case, wrap the set's contents in the `{"@set": ...}` form.
      case s => writeTagged(out, SetRefTag, writeSetContents(s))
    }
  }
}

object RenderError {

  @tailrec
  def encodeErrors(err: Error, version: APIVersion): String = {
    // for older version we need to use 'class'
    val (colname, docname) = if (version >= APIVersion.V27) {
      ("collection", "document")
    } else {
      ("class", "instance")
    }

    def NativeTypeToName(clsID: CollectionID, caller: String) = clsID match {
      case DatabaseID.collID     => "Database"
      case CollectionID.collID   => s"${colname.capitalize}"
      case IndexID.collID        => "Index"
      case KeyID.collID          => "Key"
      case TokenID.collID        => "Token"
      case TaskID.collID         => "Task"
      case UserFunctionID.collID => "Function"
      case RoleID.collID         => "Role"
      case CredentialsID.collID  => "Credentials"
      case _                     =>
        getLogger().warn(s"Incomplete error message [$caller / $clsID]")
        s"other"
    }

    err match {
      case QueryNotFound    => "No query provided."
      case InvalidJSON(msg) => msg
      case InvalidExprType(expected, provided, _) =>
        expected match {
          case Nil          => s"$provided provided."
          case List(t)      => s"$t expected, $provided provided."
          case List(t1, t2) => s"$t1 or $t2 expected, $provided provided."
          case ts =>
            s"${ts.init mkString ", "}, or ${ts.last} expected, $provided provided."
        }
      /*
       * Parse Errors
       */
      case InvalidObjectExpr(_) => "Invalid object."
      case InvalidRefExpr(_)    => "Invalid ref."
      case InvalidSetExpr(_)    => "Invalid set."
      case InvalidTimeExpr(_)   => "Invalid time."
      case InvalidDateExpr(_)   => "Invalid date."
      case InvalidBytesExpr(_)  => "Invalid bytes."
      case InvalidUUIDExpr(_)   => "Invalid UUID."
      case InvalidFormExpr(keys, _) =>
        s"No form/function found, or invalid argument keys: { ${keys mkString ", "} }."
      case InvalidLambdaExpr(_) => "Invalid lambda."
      case InvalidLambdaEffect(maxEffect, _) =>
        val disallowed = if (maxEffect == Effect.Pure) {
          "read, write, or call a user function"
        } else {
          "write or call a user function"
        }

        s"Invalid lambda. Lambda in this context may not $disallowed."
      case EmptyExpr(_)                    => "Expression list cannot be empty."
      case SchemaNotFound(_, _, _)         => "Schema resource not found."
      case ParseEvalError(evalerr)         => RenderError.encodeErrors(evalerr, version)
      /*
       * Eval Errors
       */
      case TransactionAbort(desc, _) => desc
      case UnboundVariable(name, _)  => s"Variable '$name' is not defined."
      case InvalidEffect(limit, eff, _) =>
        Effect.Action.V4Action.message(eff, limit)

      case InvalidWriteTime(_) => "Cannot write outside of snapshot time."
      /*
       * InvalidArgument --> Eval Errors
       */
      case UnresolvedRefError(orig, pos) => {
        val (typeName, value) = orig match {
          case RefScope.DatabaseRef(name, _)             => ("database", name)
          case RefScope.NativeCollectionRef(_, Some(db)) => ("database", db.name)
          case RefScope.UserFunctionRef(name, _)         => ("function", name)
          case RefScope.RoleRef(name, _)                 => ("role", name)
          case RefScope.AccessProviderRef(name, _)       => ("access_provider", name)
          case RefScope.IndexRef(name, _)                => ("index", name)
          case RefScope.KeyRef(id, _)                    => ("key", id.toLong.toString)
          case RefScope.UserCollectionRef(name, _)       => (colname, name)
          case RefScope.InstanceRef(_, RefScope.UserCollectionRef(name, _)) =>
            (colname, name)
          case RefScope.SelfRef(RefScope.UserCollectionRef(name, _)) =>
            (colname, name)
          case _ =>
            throw new RuntimeException(s"Unknown unresolved ref: $orig at $pos")
        }
        s"Ref refers to undefined $typeName '$value'"
      }
      case InvalidNativeClassRefError(sname, _) =>
        s"Valid native $colname name expected, '$sname' provided."
      case MissingIdentityError(_) => "Authentication does not contain an identity"
      case InvalidJWTScopeError(_) => "Issuing JWT for an invalid scope"
      case InvalidTokenError(_)    => "Token metadata is not accessible."
      case EmptyArrayArgument(_)   => "Non-empty array expected."
      case EmptySetArgument(_)     => "Non-empty set expected."
      case InvalidLambdaArity(expected, provided, _) =>
        s"Lambda expects an array with $expected elements. Array contains $provided."
      case InvalidArgument(expected, provided, _) =>
        expected match {
          case Nil          => s"$provided provided."
          case List(t)      => s"$t expected, $provided provided."
          case List(t1, t2) => s"$t1 or $t2 expected, $provided provided."
          case ts =>
            s"${ts.init mkString ", "}, or ${ts.last} expected, $provided provided."
        }
      case BoundsError(argument, predicate, _) =>
        s"$argument must be $predicate"
      case InvalidRegex(argument, _) =>
        s"Search pattern /$argument/ is not a valid regular expression."
      case InvalidMatchTermExpr(provided, _) =>
        s"Scalar expected, $provided provided."
      case InvalidScopedRef(_) => "Cannot write a scoped ref."
      case InvalidMatchTermArgument(provided, _) =>
        s"Scalar expected, $provided provided."
      case InvalidSetArgument(_, _, _) => "Sets are not compatible."
      case InvalidHistoricalSet(_, _) =>
        "Set does not support events history."
      case InvalidCursorObject(_) =>
        "Invalid cursor object. Expected an object with either 'before' or 'after' key in it."
      case InvalidEventCursorForm(keys, _) =>
        s"Cannot create event cursor with keys ${keys mkString ", "}"
      case InvalidDocAction(provided, _) =>
        s"String 'create', 'update', or 'delete' expected, $provided provided."
      case InvalidSetAction(provided, _) =>
        s"String 'add', or 'remove' expected, $provided provided."
      case InvalidNormalizer(provided, _) =>
        s"String 'NFKCCaseFold', 'NFC', 'NFD', 'NFKC', or 'NFKD' expected, $provided provided."
      case InvalidCreateClassArgument(_, _) =>
        s"Cannot create a schema meta $colname."
      case InvalidUpdateRefArgument(_, _) =>
        s"Cannot update a schema meta $colname."
      case InvalidDeleteRefArgument(_, _) =>
        s"Cannot delete a schema meta $colname."
      case InvalidInsertRefArgument(_) =>
        "Cannot insert event for specified ref."
      case InvalidRemoveRefArgument(_) =>
        "Cannot remove event for specified ref."
      case InvalidValidReadTime(ts, _) => s"Cannot read at valid time $ts, as it is greater than the transaction snapshot time."
      case InvalidTimeArgument(str, _) => s"Cannot cast '$str' to a time."
      case InvalidTimeUnit(unit, _)    => s"Invalid time unit '$unit'."
      case InvalidDateArgument(str, _) => s"Cannot cast '$str' to a date."
      case InvalidSchemaClassArgument(_, _) =>
        "Regular Class Ref expected, Schema Class Ref provided."
      case NonNumericSubIDArgument(idStr, _) =>
        s"Number or numeric String expected, non-numeric '$idStr' provided."
      case SubIDArgumentTooLarge(idStr, _) =>
        s"The id provided is too large: '$idStr'"
      case DivideByZero(_)          => "Illegal division by zero."
      case InvalidCast(lit, typ, _) => s"Cannot cast ${lit.rtype} to $typ."
      case InvalidConversionFormatError(conversion, _) => s"Invalid conversion format '$conversion'."
      case InvalidDateTimeSuffixError(suffix, _)       => s"Invalid date/time suffix '$suffix'."
      case IncompatibleTimeArguments(_) => "Incompatible time arguments."
      case FunctionCallError(_, _, _) =>
        "Calling the function resulted in an error."
      case LambdaStackOverflowError(_, _) =>
        s"Call stack reached the maximum limit of ${EvalContext.MaxStackDepth}."
      case FunctionStackOverflowError(_, _) =>
        s"Call stack reached the maximum limit of ${EvalContext.MaxStackDepth}."
      case PermissionDenied(_, _) =>
        "Insufficient privileges to perform the action."
      case AuthenticationFailed(_) =>
        s"The $docname was not found or provided password was incorrect."
      case ValueNotFound(path, _) => {
        val es = path map {
          case Left(l)  => l.toString
          case Right(s) => s
        }
        s"""Value not found at path ${es.mkString("[", ",", "]")}."""
      }
      case NativeCollectionModificationProhibited(clsID, _) =>
        val ctype = NativeTypeToName(clsID,"NativeCollectionModificationProhibited")
        s"Modification to native collection $ctype is prohibited."
      case CollectionAndDocumentCreatedInSameTx(_, _) =>
        s"Cannot create a collection and a document in it in the same transaction."
      case NonStreamableType(rtype, _) =>
        s"Expected a Document Ref or Version, or a Set Ref, got $rtype."
      case InvalidFQL4Value(provided, _) =>
        s"v10 value has type $provided, which cannot be represented in v4."
      case InstanceNotFound(resource, _) =>
        resource match {
          case Left(_)                                           => "Set not found."
          case Right(RefL(_, DocID(_, NativeCollectionID(native)))) =>
            val ctype = NativeTypeToName(native.collID, "InstanceNotFound")
            s"$ctype not found."
          case Right(_) => s"${docname.capitalize} not found."
        }
      case IndexInactive(name, _) => s"Index '$name' is not active."
      case KeyNotFound(secret, _) => s"No key found for secret $secret."
      case InstanceAlreadyExists(provided, _) =>
        provided match {
          case DocID(_, NativeCollectionID(native)) =>
            val ctype = NativeTypeToName(native.collID, "InstanceAlreadyExists")
            s"$ctype already exists."
          case _ => s"${docname.capitalize} already exists."
        }
      case ValidationError(_, _) =>
        s"$docname data is not valid."
      case CheckConstraintEvalError(inner, _) =>
        inner.msg
      case SchemaValidationError(msg, _) =>
        msg
      case InstanceNotUnique(_, _) => s"$docname is not unique."
      case InvalidObjectInContainer(_) => "Object is not allowed in a container"
      case MoveDatabaseError(src, dst, reason, _) =>
        s"Cannot move database '$src' into parent '$dst': $reason"
      case MixedArguments(types, _) =>
        val expected = types match {
          case Nil          => ""
          case List(t)      => s", expected $t"
          case List(t1, t2) => s", expected $t1 or $t2"
          case ts           => s", expected ${ts.init mkString ", "}, or ${ts.last}"
        }
        s"Arguments cannot be of different types$expected."
      case InvalidURLParam(name, desc) => s"Invalid url parameter $name. $desc."
      case FeatureNotAvailable(feature, _) =>
        s"The feature '$feature' is not available for your plan."
    }
  }

  def encodeErrors(err: ValidationException, version: APIVersion): String = {
    /* for older version we need to use 'class' */
    val colname = if (version > APIVersion.V21) {
      "collection"
    } else {
      "class"
    }

    err match {
      case ValueRequired(_) => "Required value not found."
      case FieldNotAllowed(_) => "Field not allowed."
      case InvalidType(_, expected, actual, _) =>
        s"Invalid type ${actual}, expected type ${expected}."
      case MultipleWildcards(_) => "Too many wildcards."
      case MultipleClassBindings(_) =>
        s"A $colname ref may only match one binding."
      case MixedWildcards(_) =>
        s"Cannot mix $colname refs and wildcards without providing bindings."
      case DuplicateValue(_) => "Value is not unique."
      case OutOfBoundValue(_, actual, min, max) =>
        s"Value $actual is not in expected range $min to $max."
      case _: InvalidNegative => "Value must be non-negative."
      case MissingField(path, alternatives @ _*) =>
        s"Must provide one of the following: ${(path +: alternatives) map {
          _.mkString(".")
        } mkString (", ")}"
      case DuplicateCachedValue(_, ttlSecs) =>
        s"Value is cached. Please wait at least $ttlSecs seconds after creating or renaming a $colname or index before reusing its name."
      case InvalidReference(_)       => "Cannot read reference."
      case InvalidScopedReference(_) => "Reference cannot be scoped."
      case InaccessibleReference(_)  => "Cannot read reference."
      case ReferenceMissing(_)       => "Reference was not found."
      case InvalidSourceBinding(_) =>
        "Field binding must be a pure unary function."
      case UnusedSourceBinding(_) =>
        "Source binding is not used as a term or value."
      case IndexShapeChange.Partition(prev, proposed) =>
        s"Index partition count may not be updated ($prev -> $proposed)."
      case IndexShapeChange.Fields(diff) =>
        s"Index sources, terms, and values may not be updated ($diff)"
      case InvalidArity(_, expected, actual) =>
        s"Found lambda with arity $actual expected $expected"
      case ResourcesExceeded(_, resource, limit) =>
        s"Maximum number of $resource exceeded. Maximum allowed: $limit."
      case ReservedName(_, name, reserved) =>
        s"Invalid name $name. Reserved names: ${reserved mkString ", "}."
      case InvalidCharacters(_, name, validRegex) =>
        s"The name $name uses invalid characters. Valid characters must match: $validRegex"
      case InvalidSecret(path) => s"Invalid secret at `$path`"
      case InvalidTransformValue(_, str) =>
        s"Invalid value `$str` at `${err.jsPath}`"
      case InvalidPassword(_) => s"Invalid password at `${err.jsPath}`."
      case ContainerCandidateContainsData(_) => "cannot make database a container while it contains collections, indexes, or user functions"
      case InvalidURI(_, uri) => s"`$uri` is a not valid URI"
      case ValidationFailure(_, msg) => s"$msg"
    }
  }
}
