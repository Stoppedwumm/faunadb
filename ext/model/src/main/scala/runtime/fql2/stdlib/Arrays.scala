package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.values._
import scala.collection.immutable.ArraySeq

object ArrayCompanion extends CompanionObject("Array") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Array => true
    case _              => false
  }

  defStaticFunction("sequence" -> tt.Array(tt.Number))(
    "from" -> tt.Int,
    "until" -> tt.Int) { (ctx, from, until) =>
    val rng = (from.value until until.value).map(Value.Int)
    if (rng.length > ArrayPrototype.MaxSize) {
      QueryRuntimeFailure
        .ValueTooLarge(
          s"range size ${rng.size} exceeds limit ${ArrayPrototype.MaxSize}",
          ctx.stackTrace)
        .toQuery
    } else {
      Value.Array(rng.to(ArraySeq)).toQuery
    }
  }
}

object ArrayPrototype
    extends Prototype(TypeTag.Array(TypeTag.A), isPersistable = true) {
  import FieldTable.R

  val MaxSize = 16_000

  defField("length" -> tt.Number) { (_, self) =>
    Query.value(Value.Int(self.elems.length))
  }

  defAccess(tt.A)("index" -> tt.Int) { (ctx, self, idx, _) =>
    at(self, idx, ctx.stackTrace).mapT(R.Val(_))
  }

  defMethod("at" -> tt.A)("index" -> tt.Int) { (ctx, self, idx) =>
    at(self, idx, ctx.stackTrace)
  }

  defMethod("aggregate" -> tt.A)(
    "seed" -> tt.A,
    "combiner" -> tt.Function(Seq(tt.A, tt.A), tt.A)) {
    (ctx, self, seed, combiner) =>
      if (!combiner.arity.accepts(2)) {
        QueryRuntimeFailure
          .InvalidFuncParamArity("combiner", 2, combiner.arity, ctx.stackTrace)
          .toQuery
      } else {
        // This halves the length of `elems` every iteration, so we should only
        // consume O(log(n)) stack frames. This means with an array which is as
        // long as the 64 bit integer limit, we'll only consume 63 stack frames.
        def aggregate0(elems: Seq[Value]): Query[Result[Value]] = {
          val seq = elems.grouped(2)
          seq
            .map {
              case Seq(a, b) =>
                ctx.evalApply(combiner, IndexedSeq[Value](a, b))
              case Seq(a) =>
                Query.value(Result.Ok(a))
              case _ => sys.error("grouped(2) gave an invalid result")
            }
            .toSeq
            .sequenceT
            .flatMapT { elems =>
              if (elems.lengthIs == 1) {
                Query.value(Result.Ok(elems(0)))
              } else {
                aggregate0(elems)
              }
            }
        }
        if (self.elems.isEmpty) {
          Result.Ok(seed).toQuery
        } else {
          aggregate0(self.elems).flatMapT { value =>
            ctx.evalApply(combiner, IndexedSeq[Value](value, seed))
          }
        }
      }
  }

  defMethod("any" -> tt.Boolean)("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) => anyImpl(ctx, self, predicate, Value.False)
  }

  defMethod("append" -> tt.Array(tt.A))("element" -> tt.A) { (_, self, elem) =>
    Value.Array(self.elems.appended(elem)).toQuery
  }

  defMethod("concat" -> tt.Array(tt.Union(tt.A, tt.B)))("other" -> tt.Array(tt.B)) {
    (_, self, other) =>
      Value.Array(self.elems.concat(other.elems)).toQuery
  }

  defMethod("distinct" -> tt.Array(tt.A))() { (_, self) =>
    Value.Array(self.elems.distinct).toQuery
  }

  defMethod("drop" -> tt.Array(tt.A))("amount" -> tt.Int) { (_, self, count) =>
    Value.Array(self.elems.drop(count.value)).toQuery
  }

  defMethod("every" -> tt.Boolean)("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) => anyImpl(ctx, self, predicate, Value.True)
  }

  defMethod("entries" -> tt.Array(tt.Tuple(tt.Number, tt.A)))() { (_, self) =>
    Value
      .Array(
        self.elems.zipWithIndex
          .map { case (v, idx) => Value.Array(Value.Number(idx), v) }
          .to(ArraySeq))
      .toQuery
  }

  defMethod("filter" -> tt.Array(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) =>
      Result.guardM {
        if (!predicate.arity.accepts(1)) {
          Result.fail(QueryRuntimeFailure
            .InvalidFuncParamArity("predicate", 1, predicate.arity, ctx.stackTrace))
        }
        self.elems.view
          .map { elem =>
            ctx.evalApply(predicate, IndexedSeq[Value](elem)) mapT {
              case Value.True                  => Some(elem)
              case Value.False | Value.Null(_) => None
              case _ =>
                Result.fail(QueryRuntimeFailure
                  .InvalidType(ValueType.BooleanType, elem, ctx.stackTrace))
            }
          }
          .sequenceT
          .mapT { list =>
            Value.Array(list.collect { case Some(value) => value }.to(ArraySeq))
          }
      }
  }

  defMethod("first" -> tt.Optional(tt.A))() { (ctx, self) =>
    Result
      .Ok(
        self.elems.headOption.getOrElse(Value.Null(Value.Null.Cause
          .NoSuchElement("empty array", ctx.stackTrace.currentStackFrame))))
      .toQuery
  }

  defMethod("firstWhere" -> tt.Optional(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      where(ctx, self, pred).mapT {
        _.headOption.getOrElse(
          Value.Null(Value.Null.Cause
            .NoSuchElement("no element found", ctx.stackTrace.currentStackFrame)))
      }
  }

  // FIXME: Once we have where predicates, fix this type signature.
  defMethod("flatten" -> tt.Array(tt.Any))() { (ctx, self) =>
    Result.guardM {
      Value
        .Array(
          self.elems.view
            .map {
              case arr: Value.Array => arr.elems
              case elem =>
                Result.fail(QueryRuntimeFailure
                  .InvalidType(ValueType.AnyArrayType, elem, ctx.stackTrace))
            }
            .flatten
            .to(ArraySeq))
        .toQuery
    }
  }

  defMethod("flatMap" -> tt.Array(tt.B))(
    "mapper" -> tt.Function(Seq(tt.A), tt.Array(tt.B))) { (ctx, self, fn) =>
    Result.guardM {
      if (!fn.arity.accepts(1)) {
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity("mapper", 1, fn.arity, ctx.stackTrace))
      }
      self.elems.view
        .map { elem =>
          ctx.evalApply(fn, IndexedSeq[Value](elem)) mapT {
            case arr: Value.Array => arr.elems
            case invalid =>
              Result.fail(QueryRuntimeFailure
                .InvalidType(ValueType.AnyArrayType, invalid, ctx.stackTrace))
          }
        }
        .sequenceT
        .mapT { list => Value.Array(list.flatten.to(ArraySeq)) }
    }
  }

  defMethod("fold" -> tt.B)(
    "seed" -> tt.B,
    "reducer" -> tt.Function(Seq(tt.B, tt.A), tt.B)) { (ctx, self, seed, reducer) =>
    if (!reducer.arity.accepts(2)) {
      QueryRuntimeFailure
        .InvalidFuncParamArity("reducer", 2, reducer.arity, ctx.stackTrace)
        .toQuery
    } else {
      self.elems.view.foldLeft(seed.toQuery) { (acc, v) =>
        acc flatMapT { acc =>
          ctx.evalApply(reducer, IndexedSeq[Value](acc, v))
        }
      }
    }
  }

  defMethod("foldRight" -> tt.B)(
    "seed" -> tt.B,
    "reducer" -> tt.Function(Seq(tt.B, tt.A), tt.B)) { (ctx, self, seed, reducer) =>
    if (!reducer.arity.accepts(2)) {
      QueryRuntimeFailure
        .InvalidFuncParamArity("reducer", 2, reducer.arity, ctx.stackTrace)
        .toQuery
    } else {
      self.elems.view.foldRight(seed.toQuery) { (v, acc) =>
        acc flatMapT { acc =>
          ctx.evalApply(reducer, IndexedSeq[Value](acc, v))
        }
      }
    }
  }

  defMethod("forEach" -> tt.Null)("callback" -> tt.Function(Seq(tt.A), tt.Any)) {
    (ctx, self, fn) =>
      if (!fn.arity.accepts(1)) {
        QueryRuntimeFailure
          .InvalidFuncParamArity("fn", 1, fn.arity, ctx.stackTrace)
          .toQuery
      } else {
        self.elems.view
          .map { elem => ctx.evalApply(fn, IndexedSeq[Value](elem)) }
          .sequenceT
          .mapT { _ => Value.Null(ctx.stackTrace.currentStackFrame) }
      }
  }

  defMethod("indexOf" -> tt.Optional(tt.Int))("element" -> tt.A) {
    (ctx, self, elem) =>
      self.elems.indexOf(elem) match {
        case v if v >= 0 => Value.Int(v).toQuery
        case _ =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
            .toQuery
      }
  }

  defMethod("indexOf" -> tt.Optional(tt.Int))("element" -> tt.A, "start" -> tt.Int) {
    (ctx, self, elem, start) =>
      checkIndex(self, start, ctx.stackTrace).map { start =>
        self.elems.indexOf(elem, start) match {
          case v if v >= 0 => Value.Int(v)
          case _ =>
            Value.Null.noSuchElement(
              "no element found",
              ctx.stackTrace.currentStackFrame)
        }
      }.toQuery
  }

  defMethod("indexWhere" -> tt.Optional(tt.Int))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) =>
      indexWhere(ctx, self, predicate, 0).mapT {
        case Some(v) => Value.Int(v)
        case None =>
          Value.Null.noSuchElement(
            "no element found",
            ctx.stackTrace.currentStackFrame)
      }
  }

  defMethod("indexWhere" -> tt.Optional(tt.Int))(
    "predicate" -> tt.Predicate(tt.A),
    "start" -> tt.Int) { (ctx, self, predicate, start) =>
    Query.value(checkIndex(self, start, ctx.stackTrace)).flatMapT { start =>
      indexWhere(ctx, self, predicate, start).mapT {
        case Some(v) => Value.Int(v)
        case None =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
      }
    }
  }

  defMethod("isEmpty" -> tt.Boolean)() { (_, self) =>
    Value.Boolean(self.elems.isEmpty).toQuery
  }

  defMethod("includes" -> tt.Boolean)("element" -> tt.A) { (_, self, value) =>
    Value.Boolean(self.elems.exists { _ == value }).toQuery
  }

  defMethod("last" -> tt.Optional(tt.A))() { (ctx, self) =>
    Result
      .Ok(
        self.elems.lastOption.getOrElse(Value.Null(Value.Null.Cause
          .NoSuchElement("empty array", ctx.stackTrace.currentStackFrame))))
      .toQuery
  }

  defMethod("lastIndexOf" -> tt.Optional(tt.Int))("element" -> tt.A) {
    (ctx, self, elem) =>
      self.elems.lastIndexOf(elem) match {
        case v if v >= 0 => Value.Int(v).toQuery
        case _ =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
            .toQuery
      }
  }

  defMethod("lastIndexOf" -> tt.Optional(tt.Int))(
    "element" -> tt.A,
    "end" -> tt.Int) { (ctx, self, elem, end) =>
    checkIndex(self, end, ctx.stackTrace).map { end =>
      self.elems.lastIndexOf(elem, end) match {
        case v if v >= 0 => Value.Int(v)
        case _ =>
          Value.Null.noSuchElement(
            "no element found",
            ctx.stackTrace.currentStackFrame)
      }
    }.toQuery
  }

  defMethod("lastIndexWhere" -> tt.Optional(tt.Int))(
    "predicate" -> tt.Predicate(tt.A)) { (ctx, self, predicate) =>
    lastIndexWhere(ctx, self, predicate, self.elems.length).mapT {
      case Some(v) => Value.Int(v)
      case None =>
        Value.Null.noSuchElement(
          "no element found",
          ctx.stackTrace.currentStackFrame)
    }
  }

  defMethod("lastIndexWhere" -> tt.Optional(tt.Int))(
    "predicate" -> tt.Predicate(tt.A),
    "end" -> tt.Int) { (ctx, self, predicate, end) =>
    Query.value(checkIndex(self, end, ctx.stackTrace)).flatMapT { end =>
      lastIndexWhere(ctx, self, predicate, end).mapT {
        case Some(v) => Value.Int(v)
        case None =>
          Value.Null
            .noSuchElement("no element found", ctx.stackTrace.currentStackFrame)
      }
    }
  }

  defMethod("lastWhere" -> tt.Optional(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      where(ctx, self, pred).mapT {
        _.lastOption.getOrElse(
          Value.Null(Value.Null.Cause
            .NoSuchElement("no element found", ctx.stackTrace.currentStackFrame)))
      }
  }

  defMethod("map" -> tt.Array(tt.B))("mapper" -> tt.Function(Seq(tt.A), tt.B)) {
    (ctx, self, fn) =>
      if (!fn.arity.accepts(1)) {
        QueryRuntimeFailure
          .InvalidFuncParamArity("mapper", 1, fn.arity, ctx.stackTrace)
          .toQuery
      } else {
        self.elems.view
          .map { elem => ctx.evalApply(fn, IndexedSeq[Value](elem)) }
          .sequenceT
          .mapT { list => Value.Array(list.to(ArraySeq)) }
      }
  }

  defMethod("nonEmpty" -> tt.Boolean)() { (_, self) =>
    Value.Boolean(self.elems.nonEmpty).toQuery
  }

  defMethod("order" -> tt.Array(tt.A))
    .vararg("ordering" -> SetCompanion.orderingType) { (ctx, self, orderings) =>
      SetCompanion.collectOrderings(orderings, ctx.stackTrace) match {
        case Result.Ok(ords) =>
          self.elems
            .map(v => OrderedSet.sortElems(ctx, ords, v).mapT(v -> _))
            .sequenceT
            .mapT(elems => Value.Array(elems.sortBy(_._2).map(_._1).to(ArraySeq)))

        case res @ Result.Err(_) => res.toQuery
      }
    }

  defMethod("prepend" -> tt.Array(tt.A))("element" -> tt.A) { (_, self, elem) =>
    Value.Array(self.elems.prepended(elem)).toQuery
  }

  defMethod("reduce" -> tt.Optional(tt.A))(
    "reducer" -> tt.Function(Seq(tt.A, tt.A), tt.A)) { (ctx, self, reducer) =>
    if (!reducer.arity.accepts(2)) {
      QueryRuntimeFailure
        .InvalidFuncParamArity("reducer", 2, reducer.arity, ctx.stackTrace)
        .toQuery
    } else {
      if (self.elems.isEmpty) {
        Value.Null
          .noSuchElement("empty array", ctx.stackTrace.currentStackFrame)
          .toQuery
      } else {
        self.elems.view.foldLeft[Query[Result[Value]]](null) { (acc, v) =>
          if (acc == null) {
            v.toQuery
          } else {
            acc flatMapT { acc =>
              ctx.evalApply(reducer, IndexedSeq[Value](acc, v))
            }
          }
        }
      }
    }
  }

  defMethod("reduceRight" -> tt.Optional(tt.A))(
    "reducer" -> tt.Function(Seq(tt.A, tt.A), tt.A)) { (ctx, self, reducer) =>
    if (!reducer.arity.accepts(2)) {
      QueryRuntimeFailure
        .InvalidFuncParamArity("reducer", 2, reducer.arity, ctx.stackTrace)
        .toQuery
    } else {
      if (self.elems.isEmpty) {
        Value.Null
          .noSuchElement("empty array", ctx.stackTrace.currentStackFrame)
          .toQuery
      } else {
        self.elems.view.foldRight[Query[Result[Value]]](null) { (v, acc) =>
          if (acc == null) {
            v.toQuery
          } else {
            acc flatMapT { acc =>
              ctx.evalApply(reducer, IndexedSeq[Value](acc, v))
            }
          }
        }
      }
    }
  }

  defMethod("reverse" -> tt.Array(tt.A))() { (_, self) =>
    Value.Array(self.elems.reverse).toQuery
  }

  defMethod("slice" -> tt.Array(tt.A))("from" -> tt.Int) { (_, self, from) =>
    // NOTE: Out of bounds indexes are allowed on slice.
    Value.Array(self.elems.slice(from.value, self.elems.length)).toQuery
  }

  defMethod("slice" -> tt.Array(tt.A))("from" -> tt.Int, "until" -> tt.Int) {
    (_, self, from, until) =>
      // NOTE: Out of bounds indexes are allowed on slice.
      Value.Array(self.elems.slice(from.value, until.value)).toQuery
  }

  defMethod("take" -> tt.Array(tt.A))("limit" -> tt.Int) { (_, self, limit) =>
    Result
      .Ok(if (limit.value < self.elems.length) {
        Value.Array(self.elems.take(limit.value))
      } else {
        self
      })
      .toQuery
  }

  defMethod("toSet" -> tt.Set(tt.A))() { (_, self) =>
    ArraySet(self).toQuery
  }

  defMethod("where" -> tt.Array(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      where(ctx, self, pred).mapT { Value.Array(_: _*) }
  }

  private def indexWhere(
    ctx: FQLInterpCtx,
    self: Value.Array,
    predicate: Value.Func,
    from: Int
  ): Query[Result[Option[Int]]] = {
    Result.guardM {
      if (!predicate.arity.accepts(1)) {
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity("predicate", 1, predicate.arity, ctx.stackTrace))
      }
      self.elems.zipWithIndex
        .slice(from, self.elems.length)
        .foldLeft[Query[Result[Option[Int]]]](Query.value(Result.Ok(None))) {
          case (q, (elem, idx)) =>
            q.flatMap {
              case Result.Ok(v) if v.isEmpty =>
                ctx.evalApply(predicate, IndexedSeq[Value](elem)).flatMapT {
                  case Value.True                  => Result.Ok(Some(idx)).toQuery
                  case Value.False | Value.Null(_) => Result.Ok(None).toQuery
                  case v =>
                    Result.fail(QueryRuntimeFailure
                      .InvalidType(ValueType.BooleanType, v, ctx.stackTrace))
                }
              case value @ _ => Query.value(value)
            }
        }
    }
  }

  private def lastIndexWhere(
    ctx: FQLInterpCtx,
    self: Value.Array,
    predicate: Value.Func,
    end: Int
  ): Query[Result[Option[Int]]] = {
    Result.guardM {
      if (!predicate.arity.accepts(1)) {
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity("predicate", 1, predicate.arity, ctx.stackTrace))
      }
      self.elems.zipWithIndex
        .slice(0, end + 1)
        .foldRight[Query[Result[Option[Int]]]](Query.value(Result.Ok(None))) {
          case ((elem, idx), q) =>
            q.flatMap {
              case Result.Ok(v) if v.isEmpty =>
                ctx.evalApply(predicate, IndexedSeq[Value](elem)).flatMapT {
                  case Value.True                  => Result.Ok(Some(idx)).toQuery
                  case Value.False | Value.Null(_) => Result.Ok(None).toQuery
                  case v =>
                    Result.fail(QueryRuntimeFailure
                      .InvalidType(ValueType.BooleanType, v, ctx.stackTrace))
                }
              case value @ _ => Query.value(value)
            }
        }
    }
  }

  private def where(
    ctx: FQLInterpCtx,
    self: Value.Array,
    pred: Value.Func
  ): Query[Result[List[Value]]] = {
    if (!pred.arity.accepts(1)) {
      Result
        .Err(
          QueryRuntimeFailure
            .InvalidFuncParamArity("pred", 1, pred.arity, ctx.stackTrace))
        .toQuery
    } else {
      self.elems
        .map { v =>
          ctx.evalApply(pred, ArraySeq(v)).map {
            case Result.Ok(Value.Boolean(t)) => Result.Ok(Option.when(t)(v))
            case Result.Ok(Value.Null(_))    => Result.Ok(None)
            case Result.Ok(v)                =>
              // FIXME: this error is not super helpful as if we were able
              // to point to the return expression of `predicate` and point out
              // it's not a boolean. Type checking will do this, but it would be
              // reaaally nice to do it at runtime, too.
              Result.Err(QueryRuntimeFailure
                .InvalidType(ValueType.BooleanType, v, ctx.stackTrace))
            case res @ Result.Err(_) => res
          }
        }
        .sequenceT
        .mapT { _.flatten }
    }
  }

  /** Implements `any` and `every`. `anyEveryImpl` sounds worse :P
    */
  private def anyImpl(
    ctx: FQLInterpCtx,
    self: Value.Array,
    predicate: Value.Func,
    default: Value.Boolean): Query[Result[Value.Boolean]] =
    Result.guardM {
      if (!predicate.arity.accepts(1)) {
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity("predicate", 1, predicate.arity, ctx.stackTrace))
      }
      self.elems
        .foldLeft[Query[Result[Value.Boolean]]](Query.value(Result.Ok(default))) {
          case (q, elem) =>
            q.flatMap {
              case Result.Ok(v) if v == default =>
                ctx.evalApply(predicate, IndexedSeq[Value](elem)).flatMapT {
                  case v @ Value.Boolean(_) => Result.Ok(v).toQuery
                  case Value.Null(_) => Result.Ok(Value.Boolean(false)).toQuery
                  case v =>
                    Result.fail(QueryRuntimeFailure
                      .InvalidType(ValueType.BooleanType, v, ctx.stackTrace))
                }
              case value @ _ => Query.value(value)
            }
        }
    }

  private def at(
    self: Value.Array,
    idx: Value.Int,
    stackTrace: FQLInterpreter.StackTrace) =
    checkIndex(self, idx, stackTrace).map { idx => self.elems(idx) }.toQuery

  private def checkIndex(
    self: Value.Array,
    idx: Value.Int,
    stackTrace: FQLInterpreter.StackTrace): Result[Int] = {
    if (idx.value < 0 || idx.value >= self.elems.length) {
      // Use the original index in the error message.
      Result.Err(
        QueryRuntimeFailure
          .IndexOutOfBounds(idx.value, self.elems.length, stackTrace))
    } else {
      Result.Ok(idx.value)
    }
  }
}
