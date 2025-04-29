package fauna.ast

import fauna.atoms._
import fauna.auth.Auth
import fauna.flags.CheckIndexConsistency
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.account.Account
import fauna.model.schema.{ NamedCollectionID, NativeCollectionID }
import fauna.model.EventSet._
import fauna.repo._
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ir._
import scala.annotation.unused

sealed trait ReadConfig {

  def config: IOConfig

  final def get(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp,
    pos: Position): Query[R[Literal]] = {

    val permQ = ec.auth.checkReadPermission(scope, id) map {
      if (_) Right(()) else Left(List(PermissionDenied(Right(RefL(scope, id)), pos)))
    }

    val getQ = get(scope, id, ts) map {
      _ toRight List(InstanceNotFound(Right(RefL(scope, id)), pos))
    }

    (permQ, getQ) parT { (_, vers) =>
      Query.value(Right(VersionL(vers)))
    }
  }

  final def exists(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp): Query[R[BoolL]] = {

    val permQ = ec.auth.checkReadPermission(scope, id) map { Right(_) }

    val getQ = get(scope, id, ts) map { opt =>
      Right(opt.isDefined)
    }

    (permQ, getQ) parT { (canRead, exists) =>
      Query.value(Right(BoolL(canRead && exists)))
    }
  }

  private def isIndexConsistencyCheckEnabled(scope: ScopeID): Query[Boolean] =
    Account.flagForScope(scope, CheckIndexConsistency)

  // Provide consistency check document and index history for context.
  private def getConsistencyCheckContext(row: IndexRow): Query[String] = {
    val count = 5
    val scope = row.key.scope
    val id = row.value.docID
    // Bit of doc history.
    // Store.rawVersions is necessary to get the transaction time.
    Store.rawVersions(scope, id, pageSize = count).takeT(count).flattenT map { vs =>
      if (vs.isEmpty) {
        "no document versions available"
      } else {
        val items = vs map { v => s"${v.action}@${v.ts}" } mkString (", ")
        s"last $count doc history items: $items"
      }
    }
  }

  private def get(scope: ScopeID, id: DocID, ts: Timestamp) = {
    RuntimeEnv.Default.Store(scope).get(id, ts) flatMap {
      case None =>
        isIndexConsistencyCheckEnabled(scope) flatMap {
          case false => Query.none
          case true =>
            Query.stats flatMap { stats =>
              // Performing the consistency check here ensures that we skip system
              // reads, but is still low-level enough to be a central codepath for
              // all relevant user reads
              Store.invalidIndexRowsForDocID(scope, id)(
                Collection.deriveMinValidTime) flatMap { indexRows =>
                indexRows map { case (mvt, i) =>
                  getConsistencyCheckContext(i) map { ctx =>
                    getLogger.warn(
                      s"Index consistency failure: ${i.value.docID} not found for $i @ ts=$ts mvt=$mvt: $ctx")
                    stats.incr("Index.Consistency.InvalidEntries")
                  }
                } sequence
              } flatMap { _ => Query.none }
            }
        }
      case v => Query.value(v)
    } selectMT isEnabled
  }

  protected def isEnabled(@unused v: Version): Query[Boolean] = Query.True
}

object DatabaseReadConfig extends ReadConfig {
  val config = DatabaseReadIOConfig

  override protected def isEnabled(v: Version): Query[Boolean] =
    Database.getUncached(v.parentScopeID, v.id.as[DatabaseID]).flatMap {
      case Some(db) => Database.isDisabled(db.scopeID).map(!_)
      case None     => Query.False
    }
}

object KeyReadConfig extends ReadConfig {
  val config = KeyIOConfig
}

object AccessProviderReadConfig extends ReadConfig {
  val config = AccessProviderIOConfig
}

object ClassReadConfig extends ReadConfig {
  val config = CollectionIOConfig
}

object TokenReadConfig extends ReadConfig {
  val config = TokenIOConfig
}

object IndexReadConfig extends ReadConfig {
  val config = IndexIOConfig
}

object UserFunctionReadConfig extends ReadConfig {
  val config = UserFunctionIOConfig
}

object RoleReadConfig extends ReadConfig {
  val config = RoleIOConfig
}

object CredentialsReadConfig extends ReadConfig {
  val config = CredentialsIOConfig
}

object TaskReadConfig extends ReadConfig {
  val config = TaskIOConfig
}

class InstanceReadConfig(val collectionID: CollectionID) extends ReadConfig {
  val config = DocumentIOConfig
}

object ReadAdaptor {
  val MaxPageSize = 100_000
  private val log = getLogger
  def apply(cls: CollectionID): ReadAdaptor =
    cls match {
      case DatabaseID.collID       => new ReadAdaptor(DatabaseReadConfig)
      case KeyID.collID            => new ReadAdaptor(KeyReadConfig)
      case TokenID.collID          => new ReadAdaptor(TokenReadConfig)
      case CollectionID.collID     => new ReadAdaptor(ClassReadConfig)
      case IndexID.collID          => new ReadAdaptor(IndexReadConfig)
      case CredentialsID.collID    => new ReadAdaptor(CredentialsReadConfig)
      case TaskID.collID           => new ReadAdaptor(TaskReadConfig)
      case UserFunctionID.collID   => new ReadAdaptor(UserFunctionReadConfig)
      case RoleID.collID           => new ReadAdaptor(RoleReadConfig)
      case AccessProviderID.collID => new ReadAdaptor(AccessProviderReadConfig)
      case UserCollectionID(id)    => new ReadAdaptor(new InstanceReadConfig(id))
      case id: CollectionID =>
        throw new IllegalArgumentException(s"Unknown collection $id.")
    }

  def getCredentials(
    scope: ScopeID,
    id: DocID,
    pos: Position): Query[R[Credentials]] =
    statReads(Query.incrDocuments(_)) {
      Credentials.getByDocument(scope, id) map {
        case Some(creds) => Right(creds)
        case None        => Left(List(AuthenticationFailed(pos)))
      }
    }

  private def countSets(set: EventSet): Int = {
    def count(sets: Seq[EventSet]) =
      sets.foldLeft(0) { (x, s) =>
        x + countSets(s)
      }

    set match {
      case Union(sets)        => count(sets)
      case Difference(sets)   => count(sets)
      case Intersection(sets) => count(sets)
      case _                  => 1
    }
  }

  def setForPaginate(
    auth: Auth,
    set: EventSet,
    pos: => Position): Query[R[EventSet]] =
    setForRead(auth, set, pos)

  def setForRead(auth: Auth, set: EventSet, pos: => Position): Query[R[EventSet]] =
    set.filteredForRead(auth) map {
      case Some(s) => Right(s)
      case None    => Left(List(PermissionDenied(Left(set), pos)))
    }

  def getInternal(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp): Query[R[Literal]] =
    apply(id.collID).get(ec, scope, id, ts, RootPosition)

  def getInstance(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp,
    pos: Position): Query[R[Literal]] =
    statReads(Query.incrDocuments(_)) {
      apply(id.collID).get(ec, scope, id, ts, pos)
    }

  def getSet(
    ec: EvalContext,
    set: EventSet,
    ts: Timestamp,
    pos: Position): Query[R[Literal]] = {

    setForRead(ec.auth, set, pos at "get") flatMapT { set =>
      statReads(Query.addSets(1, set.count, _)) {
        set.snapshotHead(ec.atValidTime(ts)) flatMap {
          case Some((scope, id)) => apply(id.collID).get(ec, scope, id, ts, pos)
          case None => Query(Left(List(InstanceNotFound(Left(set), pos))))
        }
      }
    }
  }

  def exists(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp): Query[R[BoolL]] =
    statReads(Query.incrDocuments(_)) {
      apply(id.collID).exists(ec, scope, id, ts)
    }

  def exists(
    ec: EvalContext,
    set: EventSet,
    ts: Timestamp,
    pos: Position): Query[R[BoolL]] = {

    setForRead(ec.auth, set, pos at "exists") flatMapT { set =>
      statReads(Query.addSets(1, set.count, _)) {
        set.snapshotHead(ec.atValidTime(ts)) flatMap {
          case Some((scope, id)) => apply(id.collID).exists(ec, scope, id, ts)
          case None              => Query(Right(FalseL))
        }
      }
    }
  }

  def containsValue(
    ec: EvalContext,
    set: EventSet,
    value: Literal,
    pos: Position): Query[R[Boolean]] = {

    val cursorR = CursorParser.lowerBoundCursor(
      set,
      value,
      ec.scopeID,
      ec.apiVers,
      pos at "contains_value")

    cursorR match {
      case Right(cursor) =>
        ReadAdaptor.paginate(
          set,
          Some(cursor),
          None,
          Some(1),
          None,
          true,
          ec,
          pos) mapT { case PageL(elems, _, _, _) =>
          elems contains value
        }

      case Left(errors) =>
        Query.value(Left(errors))
    }
  }

  private def statReads[T](incr: Int => Query[Unit])(q: => Query[R[T]]) =
    Query.withBytesReadDelta(q) flatMap {
      case (res @ Right(_), Some(n)) => incr(n) map { _ => res }
      case (other, _)                => Query.value(other)
    }

  /** Given any `Literal` and path, return what is at that path in the literal. This method is
    * designed to be used only after "internal" fields that shouldn't be exposed to users have
    * been filtered out via methods in the `IOConfig`.
    *
    * Any changes to the underlying structure of the user-facing model must be reflected
    * here so that functions that call this directly, like `select` and `contains`, work properly.
    */
  final def valueAtPath(
    ec: EvalContext,
    container: Literal,
    path: List[Either[Long, String]],
    all: Boolean): Query[Option[Literal]] = valueAtPath0(ec, container, path, all)

  private def castTimestamp(ts: BiTimestamp) =
    ts.validTSOpt match {
      case Some(ts) => LongL(ts.micros)
      case None     => TransactionTimeMicrosL
    }

  private def flattenArray(
    ec: EvalContext,
    arr: ArrayL,
    path: List[Either[Long, String]]): Query[Option[Literal]] = {

    val elems = arr.elems map { valueAtPath(ec, _, path, true) }

    elems.sequence map { seq =>
      val b = List.newBuilder[Literal]
      seq map {
        case None                => ()
        case Some(ArrayL(elems)) => b ++= elems
        case Some(r)             => b += r
      }
      Some(ArrayL(b.result()))
    }
  }

  /* instance and class needs to be maintained for application backwards
   * compatibility functionality. In addition instance is written to the disk. */
  @annotation.tailrec
  private def valueAtPath0(
    ec: EvalContext,
    container: Literal,
    path: List[Either[Long, String]],
    all: Boolean): Query[Option[Literal]] = {

    container match {
      case r: RefL =>
        path match {
          case Nil =>
            Query.some(r)

          case Right("class" | "collection") :: rest =>
            val ref = RefL(r.scope, r.id.collID.toDocID)
            if (rest.isEmpty) {
              Query.some(ref)
            } else {
              valueAtPath0(ec, ref, rest, all)
            }

          case Right("id") :: Nil =>
            r.id.collID match {
              case CollectionID.collID =>
                r.id.as[CollectionID] match {
                  case NativeCollectionID(_) =>
                    Query.value(
                      NativeCollectionID
                        .toName(r.id.as[CollectionID], ec.apiVers)) mapT {
                      StringL(_)
                    }
                  case UserCollectionID(_) =>
                    SchemaNames.lookupCachedName(r.scope, r.id) mapT { StringL(_) }

                  case id: CollectionID =>
                    throw new IllegalStateException(s"Unknown collection $id.")
                }
              case NamedCollectionID(_) =>
                SchemaNames.lookupCachedName(r.scope, r.id) mapT { StringL(_) }
              case _ =>
                Query.some(StringL(r.id.subID.toLong.toString))
            }

          case Right("database") :: rest =>
            if (r.scope == ec.scopeID) {
              Query.none
            } else {
              Database.forScope(r.scope) flatMap {
                case None =>
                  Query.none

                case Some(db) =>
                  val ref = RefL(db.parentScopeID, db.id.toDocID)
                  if (rest.isEmpty) {
                    Query.some(ref)
                  } else {
                    valueAtPath(ec, ref, rest, all)
                  }
              }
            }

          case Right("ref" | "ts" | "data") :: _
              if ec.apiVers >= APIVersion.Unstable =>
            RuntimeEnv.Default.Store(r.scope).get(r.id) flatMapT { version =>
              valueAtPath(ec, VersionL(version), path, all)
            }

          case _ =>
            Query.none
        }

      case s: SetL =>
        def subSet(set: EventSet): Literal =
          set match {
            case SchemaSet(scope, id) => RefL(scope, id)
            case DocSet(scope, id)    => RefL(scope, id)
            case _                    => SetL(set)
          }

        def subSets(sets: List[EventSet]): Literal = ArrayL(sets map subSet)

        path match {
          case Right("@set") :: Nil  => Query.some(s)
          case Right("@set") :: rest =>
            // ATTENTION: This feature is deprecated and will be removed in future
            // versions. DO NOT add more cases to the pattern match.
            val valueR = s.set match {
              case SchemaSet(scope, id) => RefL(scope, id)
              case DocSet(scope, id)    => RefL(scope, id)
              case Union(sets)          => ObjectL("union" -> subSets(sets.toList))
              case Intersection(sets)   => ObjectL("intersection" -> subSets(sets))
              case Difference(sets)     => ObjectL("difference" -> subSets(sets))
              case Distinct(set)        => ObjectL("distinct" -> subSet(set))

              case LambdaJoin(src, lambda, _) =>
                ObjectL("join" -> subSet(src), "with" -> lambda)

              case IndexJoin(src, idx) =>
                ObjectL(
                  "join" -> subSet(src),
                  "with" -> RefL(idx.scopeID, idx.id.toDocID))

              case IndexSet(idx, terms, _) =>
                val termsR = if (terms.size == 1) {
                  Literal.fromIndexTerm(idx.scopeID, terms.head)
                } else {
                  ArrayL(terms.toList map { Literal.fromIndexTerm(idx.scopeID, _) })
                }

                ObjectL(
                  "match" -> RefL(idx.scopeID, idx.id.toDocID),
                  "terms" -> termsR)

              case _ => NullL
            }

            valueAtPath0(ec, valueR, rest, all)

          case Nil => Query.some(s)
          case _   => Query.none
        }

      case _: ScalarL | _: LambdaL =>
        if (path.isEmpty) {
          Query.some(container)
        } else {
          Query.none
        }

      case v: VersionL =>
        val scopeID = v.version.parentScopeID
        val isIndex = v.version.docID.is[IndexID]

        path match {
          case Nil => Query.some(v)
          case Right("ref") :: rest =>
            valueAtPath0(ec, RefL(scopeID, v.version.docID), rest, all)

          case Right("class") :: rest =>
            val ref = RefL(scopeID, v.version.collID.toDocID)
            valueAtPath0(ec, ref, rest, all)

          case Right("ts") :: rest =>
            valueAtPath0(ec, castTimestamp(v.version.ts), rest, all)

          // idx sources are coded as class not collection. translate the path
          case Right("source") :: Left(idx) :: Right("collection") :: rest
              if isIndex =>
            valueAtPath0(
              ec,
              container,
              Right("source") :: Left(idx) :: Right("class") :: rest,
              all)

          // idx sources are coded as class not collection. translate the path
          case Right("source") :: Right("collection") :: rest if isIndex =>
            valueAtPath0(
              ec,
              container,
              Right("source") :: Right("class") :: rest,
              all)

          case Right(k) :: rest =>
            val readable = apply(v.version.collID).readableData(v.version)
            AList(readable.fields.elems).get(k) match {
              case Some(ir) => valueAtPath0(ec, Literal(scopeID, ir), rest, all)
              case None     => Query.none
            }

          case _ => Query.none
        }

      case a: ArrayL =>
        path match {
          case Nil if all => flattenArray(ec, a, Nil)
          case Nil        => Query.some(a)
          case Left(i) :: rest =>
            a.elems.lift(i.toInt) match {
              case Some(c) => valueAtPath0(ec, c, rest, all)
              case None    => Query.none
            }
          case path if all => flattenArray(ec, a, path)
          case _           => Query.none
        }

      case o: ObjectL =>
        path match {
          case Nil => Query.some(o)
          case Right(k) :: rest =>
            AList(o.elems).get(k) match {
              case Some(c) => valueAtPath0(ec, c, rest, all)
              case None    => Query.none
            }
          case _ => Query.none
        }

      case p: PageL =>
        path match {
          case Nil                   => Query.some(p)
          case Right("data") :: rest => valueAtPath0(ec, ArrayL(p.elems), rest, all)
          case Right("before") :: rest =>
            p.before match {
              case Some(c) => valueAtPath0(ec, c, rest, all)
              case None    => Query.none
            }
          case Right("after") :: rest =>
            p.after match {
              case Some(c) => valueAtPath0(ec, c, rest, all)
              case None    => Query.none
            }
          case Left(i) :: rest =>
            p.elems.lift(i.toInt) match {
              case Some(c) => valueAtPath0(ec, c, rest, all)
              case None    => Query.none
            }
          case _ => Query.none
        }

      case e: ElemL =>
        path match {
          case Nil                    => Query.some(e)
          case Right("value") :: rest => valueAtPath0(ec, e.value, rest, all)
          case Right("sources") :: rest =>
            valueAtPath0(ec, ArrayL(e.sources map { SetL(_) }), rest, all)
          case _ => Query.none
        }

      case e: EventL =>
        path match {
          case Nil                    => Query.some(e)
          case Right("ts") :: Nil     => Query.some(castTimestamp(e.event.ts))
          case Right("action") :: Nil => Query.some(ActionL(e.event.action))
          case Right("resource") :: Nil =>
            Query.some(RefL(e.event.scopeID, e.event.docID))
          case Right("instance" | "document") :: Nil =>
            Query.some(RefL(e.event.scopeID, e.event.docID))
          case Right("resource") :: rest =>
            valueAtPath0(ec, RefL(e.event.scopeID, e.event.docID), rest, all)
          case Right("instance" | "document") :: rest =>
            valueAtPath0(ec, RefL(e.event.scopeID, e.event.docID), rest, all)
          case Right(k) :: rest =>
            e match {
              case DocEventL(ev) =>
                val data = apply(ev.docID.collID).readableData(ev)

                AList(data.fields.elems).get(k) match {
                  case Some(ir) =>
                    valueAtPath0(ec, Literal(ec.scopeID, ir), rest, all)
                  case None => Query.none
                }
              case SetEventL(ev) if k == "data" =>
                valueAtPath0(
                  ec,
                  Literal.fromIndexTerms(ec.scopeID, ev.values),
                  rest,
                  all)
              case _ => Query.none
            }
          case _ => Query.none
        }

      case CursorL(Left(evt)) =>
        valueAtPath0(ec, evt, path, all)

      case CursorL(Right(arr)) =>
        valueAtPath0(ec, arr, path, all)
    }
  }

  def contains(
    ec: EvalContext,
    path: List[Either[Long, String]],
    container: Literal,
    @unused pos: Position) = {

    valueAtPath(ec, container, path, false) map { v =>
      Right(BoolL(v.isDefined))
    }
  }

  def select(
    ec: EvalContext,
    path: List[Either[Long, String]],
    all: Boolean,
    container: Literal,
    default: Option[Literal],
    pos: Position) = {

    valueAtPath(ec, container, path, all) map { res =>
      if (!all) {
        res orElse default toRight List(ValueNotFound(path, pos))
      } else {
        res match {
          case Some(res @ ArrayL(_)) => Right(res)
          case Some(res)             => Right(ArrayL(List(res)))
          case None                  => Right(ArrayL(List.empty))
        }
      }
    }
  }

  def paginate(
    set: EventSet,
    cursorOpt: Option[Cursor],
    tsOpt: Option[AbstractTimeL],
    sizeOpt: Option[Long],
    sourcesOpt: Option[Boolean],
    ascending: Boolean,
    ecRaw: EvalContext,
    pos: Position,
    verb: Option[String] = None): Query[R[PageL]] = {

    val ec = ecRaw.atValidTime(tsOpt)
    val size = sizeOpt.fold(DefaultPageSize) { _.min(MaxPageSize).toInt }
    val sources = sourcesOpt contains true

    if (set.shape.isHistorical) {
      PaginateEvents(ec, set, cursorOpt, ascending, size, sources, pos, verb)
    } else {
      PaginateArray(ec, set, cursorOpt, ascending, size, sources, pos, verb)
    }
  }

  private object PaginateArray extends PaginateImpl {
    private val log = getLogger

    protected def getCursorL(evt: Event): CursorL =
      CursorL(evt.toSetEvent.tuple.cursor.toList map {
        Literal.fromIndexTerm(evt.scopeID, _)
      })

    protected def dropCmp(aEvt: Event, bEvt: Event, asc: Boolean): Boolean = {
      asc ^ (aEvt.toSetEvent.tuple >= bEvt.toSetEvent.tuple)
    }

    protected def get(
      ec: EvalContext,
      set: EventSet,
      from: Event,
      to: Event,
      ascending: Boolean,
      overSize: Int): PagedQuery[Iterable[EventSet.Elem[Event]]] = {

      set.snapshot(
        ec,
        from.toSetEvent,
        to.toSetEvent,
        overSize,
        ascending) recoverWith { case e: RangeArgumentException =>
        // log and propagate error
        log.error(
          s"PaginateArray from:${from.toSetEvent} to:${to.toSetEvent} ascending:$ascending")
        Query.fail(e)
      }
    }
  }

  private object PaginateEvents extends PaginateImpl {

    protected def getCursorL(e: Event): CursorL = CursorL(EventL(e))

    protected def dropCmp(a: Event, b: Event, asc: Boolean): Boolean =
      (a, b) match {
        case (a: SetEvent, b: SetEvent) =>
          asc ^ (SetEvent.ord.compare(a, b) >= 0)
        case _ =>
          // FIXME: HistoricalOrd takes transaction time into
          // consideration, but really should ignore it.
          val a0 = a.at(a.ts.validTS)
          val b0 = b.at(b.ts.validTS)

          asc ^ (Event.HistoricalOrd.compare(a0, b0) >= 0)
      }

    protected def get(
      ec: EvalContext,
      set: EventSet,
      from: Event,
      to: Event,
      ascending: Boolean,
      overSize: Int): PagedQuery[Iterable[EventSet.Elem[Event]]] = {

      set.history(ec, from.toSetEvent, to.toSetEvent, overSize, ascending)
    }
  }

  sealed abstract class PaginateImpl {

    protected def getCursorL(t: Event): CursorL
    protected def dropCmp(a: Event, b: Event, asc: Boolean): Boolean

    protected def get(
      ec: EvalContext,
      set: EventSet,
      from: Event,
      to: Event,
      ascending: Boolean,
      overSize: Int): PagedQuery[Iterable[Elem[Event]]]

    def apply(
      ec: EvalContext,
      uncheckedSet: EventSet,
      cursor: Option[Cursor],
      ascending: Boolean,
      size: Int,
      sources: Boolean,
      pos: Position,
      verb: Option[String]): Query[R[PageL]] = {

      Query.withBytesReadDelta {
        ReadAdaptor.setForPaginate(
          ec.auth,
          uncheckedSet,
          verb.fold(pos) { pos at _ }) flatMapT { set =>
          val overSize = (size + 1) max set.MinPageSize
          val from = cursor match {
            case Some(c) => c.event
            case None =>
              if (ascending) {
                set.minValue
              } else {
                set.maxValue.at(ec.validTime)
              }
          }
          val to = if (ascending) set.maxValue.at(ec.validTime) else set.minValue
          val pageQ = get(ec, set, from, to, ascending, overSize) recoverWith {
            case e: RangeArgumentException =>
              // log and propagate error
              log.error(s"PaginateImpl pos:${verb
                  .fold(pos) { pos at _ }} scopeID:${ec.scopeID} cursor:$cursor from:$from to:$to ascending:$ascending")
              Query.fail(e)
          }

          val filteredQ = if (set.isFiltered) {
            pageQ selectMT { elem =>
              ec.auth.checkReadPermission(
                elem.value.scopeID,
                elem.value.docID
              )
            }
          } else {
            pageQ
          }

          val droppedQ = filteredQ flatMap { page =>
            val values = page dropWhile { e: Elem[Event] =>
              dropCmp(e.value, from, ascending)
            }

            values.takeT(overSize).flattenT
          }

          droppedQ map { elems =>
            val origCursor = cursor map { _.literal }
            val page = elems.toList
            val hasNext = page.lift(size).isDefined

            val next = if (size == 0) {
              origCursor
            } else {
              if (hasNext) {
                val i = if (ascending) size else size - 1
                Some(getCursorL(page(i).value))
              } else {
                None
              }
            }

            val trimmed = page take size
            val rv = if (ascending) trimmed else trimmed.reverse

            val (before, after) =
              if (ascending) (origCursor, next) else (next, origCursor)

            val valueRs = rv map { e =>
              if (sources) {
                ElemL(set.getLiteral(e.value), e.sources)
              } else {
                set.getLiteral(e.value)
              }
            }

            val rawValues = rv map { e =>
              if (sources) {
                ElemL(getCursorL(e.value), e.sources)
              } else {
                getCursorL(e.value)
              }
            }

            Right((PageL(valueRs, rawValues, before, after), set))
          }
        } recover {
          // FIXME: See explanation of why this exception exists in EventSet.scala
          case _: InvalidHistoricalSetException =>
            Left(List(InvalidHistoricalSet(uncheckedSet, pos)))
        }
      } flatMap {
        case (Left(errs), _)                   => Query.value(Left(errs))
        case (Right((page, _)), None)          => Query.value(Right(page))
        case (Right((page, set)), Some(delta)) =>
          // NB. the set has already been accounted for; record only the data
          Query.addSets(0, set.count, delta) map { _ =>
            Right(page)
          }
      }
    }
  }

  def readableData(version: Version): Data =
    apply(version.collID).readableData(version)

  def readableDataStream(version: Version): DataStream =
    apply(version.collID).readableDataStream(version)

  def readableDiffStream(event: DocEvent): DataStream =
    apply(event.docID.collID).readableDiffStream(event)
}

class ReadAdaptor(logic: ReadConfig) {

  def get(
    ec: EvalContext,
    scope: ScopeID,
    id: DocID,
    ts: Timestamp,
    pos: Position): Query[R[Literal]] = logic.get(ec, scope, id, ts, pos)

  def exists(ec: EvalContext, scope: ScopeID, id: DocID, ts: Timestamp) =
    logic.exists(ec, scope, id, ts)

  def readableData(version: Version): Data =
    logic.config.readableData(version)

  def readableData(event: DocEvent): Data =
    logic.config.readableData(event)

  def readableDataStream(version: Version): DataStream =
    logic.config.readableDataStream(version)

  def readableDiffStream(event: DocEvent): DataStream =
    logic.config.readableDiffStream(event)
}
