package fauna.ast

import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.syntax._
import fauna.model._
import fauna.model.runtime.Effect
import fauna.repo.query.Query
import fauna.storage.index.IndexTerm
import scala.annotation.unused
import scala.collection.mutable.{ Map => MMap }

object MatchFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    refE: Either[StringL, RefL],
    terms: List[IndexTerm],
    ec: EvalContext,
    pos: Position): Query[R[SetL]] =
    apply(terms, refE, ec.auth, pos)

  def apply(
    refE: Either[StringL, RefL],
    ec: EvalContext,
    pos: Position): Query[R[SetL]] =
    apply(Nil, refE, ec.auth, pos)

  def apply(
    terms: List[IndexTerm],
    refE: Either[StringL, RefL],
    ec: EvalContext,
    pos: Position): Query[R[SetL]] =
    apply(terms, refE, ec.auth, pos)

  def apply(
    terms: List[IndexTerm],
    refE: Either[StringL, RefL],
    auth: Auth,
    pos: Position): Query[R[SetL]] = {
    val refQ: Query[R[RefL]] = refE match {
      case Right(ref) => Query.value(Right(ref))
      case Left(StringL(name)) =>
        Index.idByName(auth.scopeID, name) map {
          case None =>
            val ref = RefParser.RefScope.IndexRef(name, None)
            Left(List(UnresolvedRefError(ref, pos at "index")))
          case Some(id) => Right(RefL(auth.scopeID, id))
        }
    }

    refQ flatMapT { ref =>
      auth.isInScope(ref.scope) flatMap {
        if (_) {
          val RefL(scope, IndexID(index)) = ref
          Index.get(scope, index) mapT { idx =>
            if (idx.isActive) {
              Right(SetL(IndexSet(idx, terms.toVector)))
            } else {
              Left(List(IndexInactive(idx.name, pos at "index")))
            }
          } getOrElseT Left(List(InstanceNotFound(Right(ref), pos at "index")))
        } else {
          Query(Left(List(PermissionDenied(Right(ref), pos at "index"))))
        }
      }
    }
  }
}

object SingletonFunction extends QFunction {
  val effect = Effect.Pure

  def apply(ref: RefL, @unused ec: EvalContext, pos: Position): Query[R[SetL]] =
    apply(ref, pos)

  def apply(ref: RefL, @unused auth: Auth, pos: Position): Query[R[SetL]] =
    apply(ref, pos)

  def apply(ref: RefL, @unused pos: Position): Query[R[SetL]] =
    Query.value(Right(SetL(DocSingletonSet(ref.scope, ref.id))))
}

object EventsFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    set: IdentifierL,
    @unused ec: EvalContext,
    pos: Position): Query[R[SetL]] =
    apply(set, pos)

  def apply(set: IdentifierL, @unused auth: Auth, pos: Position): Query[R[SetL]] =
    apply(set, pos)

  def apply(set: IdentifierL, pos: Position): Query[R[SetL]] = set match {
    case SetL(set) if set.shape.isHistorical =>
      Query.value(Left(List(InvalidHistoricalSet(set, pos))))

    case SetL(set) =>
      Query.value(Right(SetL(HistoricalSet(set))))

    case RefL(scope, id) =>
      Query.value(Right(SetL(HistoricalSet(DocSet(scope, id)))))
  }
}

object DocumentsFunction extends QFunction {
  val effect = Effect.Pure

  def apply(ref: RefL, @unused ec: EvalContext, pos: Position): Query[R[SetL]] =
    apply(ref, pos)

  def apply(ref: RefL, @unused auth: Auth, pos: Position): Query[R[SetL]] =
    apply(ref, pos)

  def apply(ref: RefL, @unused pos: Position): Query[R[SetL]] =
    Query.value(Right(SetL(DocumentsSet(ref.scope, ref.id.as[CollectionID]))))
}

sealed trait AssociativeSetFunction extends QFunction {
  val effect = Effect.Pure

  protected def name: String

  protected def mkSet(sets: List[EventSet]): EventSet
  protected def mkArray(arrays: List[ArrayL]): ArrayL

  private def checkSets(
    sets: List[EventSet],
    pos: Position): Either[List[EvalError], List[EventSet]] = {
    val res = sets.sliding(2) flatMap { set =>
      (set: @unchecked) match {
        case a :: Nil =>
          List(Right(a))
        case a :: b :: Nil if a.shape compatible b.shape =>
          List(Right(a), Right(b))
        case a :: b :: Nil =>
          List(Left(List(InvalidSetArgument(a, b, pos at name))))
      }
    } toList

    Parser.sequence(res)
  }

  def apply(
    iterables: List[IterableL],
    @unused ec: EvalContext,
    pos: Position): Query[R[IterableL]] =
    applyIterables(iterables, pos)

  def apply(sets: List[EventSet], pos: Position): Query[R[SetL]] =
    applyIterables(sets map SetL, pos) map {
      case Right(set: SetL) => Right(set)
      case Right(v) =>
        Left(List(InvalidArgument(List(Type.Set), v.rtype, pos at name)))
      case Left(errs) => Left(errs)
    }

  private def applyIterables(
    iterables: List[IterableL],
    pos: Position): Query[R[IterableL]] = {
    val size = iterables.size

    val sets = iterables.collect { case SetL(set) =>
      set
    }

    val arrays = iterables.collect { case array: ArrayL =>
      array
    }

    if (sets.lengthCompare(size) == 0 && arrays.isEmpty) {
      Query(checkSets(sets, pos).map { sets => SetL(mkSet(sets)) })
    } else if (sets.isEmpty && arrays.lengthCompare(size) == 0) {
      Query.value(Right(mkArray(arrays)))
    } else {
      Query.value(
        Left(List(MixedArguments(List(Type.Set, Type.Array), pos at name))))
    }
  }
}

object UnionFunction extends AssociativeSetFunction {
  protected val name = "union"
  protected def mkSet(sets: List[EventSet]) = Union(sets)

  // counts the occurrence of the values
  private def occurrenceCounts(xs: Seq[Literal]): MMap[Literal, Int] = {
    val occ = MMap.empty[Literal, Int].withDefaultValue(0)
    for (y <- xs) occ(y) += 1
    occ
  }

  protected def mkArray(arrays: List[ArrayL]): ArrayL =
    ArrayL(arrays.tail.foldLeft(arrays.head.elems) { case (left, ArrayL(right)) =>
      val occurrence = occurrenceCounts(left)

      occurrenceCounts(right).foreach { case (value, count) =>
        occurrence(value) = occurrence(value) max count
      }

      val builder = List.newBuilder[Literal]

      left.foreach { value =>
        val count = occurrence(value)
        (1 to count).foreach { _ => builder += value }
        occurrence(value) = 0
      }

      right.foreach { value =>
        val count = occurrence(value)
        (1 to count).foreach { _ => builder += value }
        occurrence(value) = 0
      }

      builder.result()
    })
}

object IntersectionFunction extends AssociativeSetFunction {
  protected val name = "intersection"
  protected def mkSet(sets: List[EventSet]) = Intersection(sets)

  protected def mkArray(arrays: List[ArrayL]): ArrayL =
    ArrayL(arrays.tail.foldLeft(arrays.head.elems) { case (acc, ArrayL(elems)) =>
      acc.intersect(elems)
    })
}

object DifferenceFunction extends AssociativeSetFunction {
  protected val name = "difference"
  protected def mkSet(sets: List[EventSet]) = Difference(sets)

  protected def mkArray(arrays: List[ArrayL]): ArrayL =
    ArrayL(arrays.tail.foldLeft(arrays.head.elems) { case (acc, ArrayL(elems)) =>
      acc.diff(elems)
    })
}

object JoinFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    source: EventSet,
    withL: Literal,
    ec: EvalContext,
    pos: Position): Query[R[SetL]] =
    withL match {
      case lambda: LambdaL =>
        apply(source, lambda, pos)
      case ref @ RefL(_, id) if id.collID == IndexID.collID =>
        apply(source, ref, ec.auth, pos)
      case UnresolvedRefL(orig) =>
        Query(Left(List(UnresolvedRefError(orig, pos at "with"))))
      case _ =>
        Query(
          Left(
            List(
              InvalidArgument(
                List(Type.Lambda, Type.IndexRef),
                withL.rtype,
                pos at "with"))))
    }

  def apply(source: EventSet, lambda: LambdaL, pos: Position): Query[R[SetL]] =
    lambda.callMaxEffect map {
      case Effect.Pure => Right(SetL(LambdaJoin(source, lambda, pos)))
      case _           => Left(List(InvalidLambdaEffect(Effect.Pure, pos at "with")))
    }

  def apply(source: EventSet, ref: RefL, auth: Auth, pos: Position): Query[R[SetL]] =
    auth.isInScope(ref.scope) flatMap {
      if (_) {
        val RefL(scope, IndexID(index)) = ref
        Index.get(scope, index) mapT { idx =>
          Right(SetL(IndexJoin(source, idx)))
        } getOrElseT Left(List(InstanceNotFound(Right(ref), pos at "with")))
      } else {
        Query(Left(List(PermissionDenied(Right(ref), pos at "with"))))
      }
    }
}

/** Returns a new iterable type (a Set, Page, or an Array) without duplicate elements
  */
object DistinctFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    source: IterableL,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    source match {
      case ArrayL(elems) =>
        Query.value(Right(ArrayL(elems.distinct)))

      case PageL(elems, unmapped, before, after) =>
        Query.value(Right(PageL(elems.distinct, unmapped, before, after)))

      case SetL(set) =>
        apply(set, pos)
    }

  def apply(source: EventSet, @unused pos: Position): Query[R[SetL]] =
    Query.value(Right(SetL(Distinct(source))))
}

object RangeFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    source: EventSet,
    from: Cursor,
    to: Cursor,
    @unused scope: ScopeID,
    @unused pos: Position): Query[R[SetL]] = {

    Query.value(
      Right(SetL(RangeSet(source, from.event.toSetEvent, to.event.toSetEvent))))
  }
}

object ReverseFunction extends QFunction {
  val effect = Effect.Pure

  def apply(
    source: IterableL,
    @unused ec: EvalContext,
    pos: Position): Query[R[Literal]] =
    source match {
      case ArrayL(elems) =>
        Query.value(Right(ArrayL(elems.reverse)))

      case PageL(elems, unmapped, before, after) =>
        Query.value(Right(PageL(elems.reverse, unmapped, before, after)))

      case SetL(set) =>
        apply(set, pos)
    }

  def apply(source: EventSet, @unused pos: Position): Query[R[SetL]] =
    Query.value(Right(SetL(ReverseSet(source))))
}
