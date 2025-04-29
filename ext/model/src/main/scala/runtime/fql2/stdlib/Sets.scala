package fauna.model.runtime.fql2.stdlib

import fauna.lang.syntax._
import fauna.model.runtime.fql2._
import fauna.repo.query.Query
import fauna.repo.schema.Path
import fauna.repo.values.{ Value, ValueReification, ValueType }
import fql.ast.{ Expr, Span }
import fql.parser.ExprParser
import scala.collection.immutable.ArraySeq

object SetPrototype
    extends Prototype(TypeTag.Set(TypeTag.A), isPersistable = false) {

  /** Only call within a guard block */
  private def checkArgArity(
    argName: String,
    accept: Int,
    fn: Value.Func,
    stackTrace: FQLInterpreter.StackTrace) =
    if (!fn.arity.accepts(accept)) {
      Result.unsafeFail(
        QueryRuntimeFailure
          .InvalidFuncParamArity(argName, accept, fn.arity, stackTrace))
    }

  private[stdlib] def checkSize(
    ctx: FQLInterpCtx,
    argName: String,
    size: Value.Int
  ): Query[Result[Unit]] =
    if (size.value <= 0 || size.value > ValueSet.MaximumElements) {
      QueryRuntimeFailure
        .InvalidBounds(
          ctx,
          argName,
          s"between 1 and ${ValueSet.MaximumElements} (inclusive)",
          size,
          ctx.stackTrace
        )
        .map { err => Result.Err(err) }
    } else {
      Query.value(Result.Ok(()))
    }

  private[stdlib] def whereImpl(
    ctx: FQLInterpCtx,
    set: ValueSet,
    pred: Value.Func
  ) =
    Result.guardM {
      checkArgArity("pred", 1, pred, ctx.stackTrace)
      WhereFilterSet(set, pred, ctx.userValidTime, ctx.stackTrace).toQuery
    }

  private[stdlib] def mapImpl(
    ctx: FQLInterpCtx,
    set: ValueSet,
    func: Value.Func
  ) =
    Result.guardM {
      checkArgArity("mapper", 1, func, ctx.stackTrace)
      ProjectedSet(set, func, ctx.userValidTime, ctx.stackTrace).toQuery
    }

  private[stdlib] def firstWhereImpl(
    ctx: FQLInterpCtx,
    set: ValueSet,
    pred: Value.Func
  ) =
    Result.guardM {
      checkArgArity("pred", 1, pred, ctx.stackTrace)
      WhereFilterSet(set, pred, ctx.userValidTime, ctx.stackTrace).first(ctx)
    }

  private[stdlib] def lastWhereImpl(
    ctx: FQLInterpCtx,
    set: ValueSet,
    pred: Value.Func
  ) =
    Result.guardM {
      checkArgArity("pred", 1, pred, ctx.stackTrace)
      WhereFilterSet(ReverseSet(set), pred, ctx.userValidTime, ctx.stackTrace)
        .first(ctx)
    }

  private def maybeEmitSetHint(
    ctx: FQLInterpCtx,
    set: ValueSet,
    methodName: String,
    span: Span): Query[Unit] =
    if (ctx.performanceDiagnosticsEnabled && set.shouldIncludePerformanceHints) {
      ctx.emitDiagnostic(Hints.FullSetRead(methodName, span))
    } else {
      Query.unit
    }

  /** evaluation: eager */
  defMethod("aggregate" -> tt.A)(
    "seed" -> tt.A,
    "combiner" -> tt.Function(Seq(tt.A, tt.A), tt.A)) {
    (ctx, self, seed, combiner) =>
      maybeEmitSetHint(ctx, self, "aggregate", ctx.stackTrace.currentStackFrame)
        .flatMap { _ =>
          Result.guardM {
            checkArgArity("combiner", 2, combiner, ctx.stackTrace)
            self.aggregate(ctx, seed, combiner)
          }
        }
  }

  /** evaluation: lazy */
  defMethod("where" -> tt.Set(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) => whereImpl(ctx, self, pred)
  }

  /** constructor for WhereFilterSet from reification */
  defMethod("$where" -> tt.Set(tt.A))(
    "predicate" -> tt.Predicate(tt.A),
    "ts" -> tt.Optional(tt.Time)) { (ctx, self, pred, validTime) =>
    Result.guardM {
      checkArgArity("pred", 1, pred, ctx.stackTrace)
      WhereFilterSet(
        self,
        pred,
        ValueSet.timeFromValue(validTime),
        // when we are coming from reification, this means the 'set' has been
        // passed over the wire and we don't want to create it with the stack
        // trace in this scenario as it will already be accounted for in the
        // Set.paginate call
        FQLInterpreter.StackTrace(Nil)
      ).toQuery
    }
  }

  /** evaluation: lazy */
  defMethod("distinct" -> tt.Set(tt.A))() { (ctx, self) =>
    maybeEmitSetHint(ctx, self, "distinct", ctx.stackTrace.currentStackFrame).map {
      _ =>
        DistinctSet(self).toResult
    }
  }

  /** evaluation: lazy */
  defMethod("drop" -> tt.Set(tt.A))("amount" -> tt.Int) { (_, self, count) =>
    DropSet(self, count.value).toQuery
  }

  /** evaluation: eager */
  defMethod("firstWhere" -> tt.Optional(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      firstWhereImpl(ctx, self, pred)
  }

  /** evaluation: lazy */
  defMethod("flatMap" -> tt.Set(tt.B))(
    "mapper" -> tt.Function(Seq(tt.A), tt.Set(tt.B))) { (ctx, self, func) =>
    if (!func.arity.accepts(1)) {
      Result
        .Err(
          QueryRuntimeFailure
            .InvalidFuncParamArity("mapper", 1, func.arity, ctx.stackTrace))
        .toQuery
    } else {
      Result
        .Ok(
          FlatMapSet(
            self,
            ValueSet.InitialPage,
            func,
            ctx.userValidTime,
            reversed = false,
            ctx.stackTrace))
        .toQuery
    }
  }

  /** constructor for FlatMapSet from reification */
  defMethod("$flatMap" -> tt.Set(tt.B))(
    "mapper" -> tt.Function(Seq(tt.A), tt.Set(tt.B)),
    "ts" -> tt.Optional(tt.Time),
    "reversed" -> tt.Boolean,
    "cont" -> tt.Optional(tt.Array(tt.Any))) {
    (_, self, mapper, validTime, reversed, cont) =>
      val cont0 = cont match {
        case Value.Null(_)   => ValueSet.InitialPage
        case Value.Array(es) => ValueSet.Continuation(es)
        case v               => throw new IllegalStateException(s"invalid cont: $v")
      }
      Result
        .Ok(
          FlatMapSet(
            self,
            cont0,
            mapper,
            ValueSet.timeFromValue(validTime),
            reversed.value,
            // when we are coming from reification, this means the 'set' has been
            // passed over the wire and we don't want to create it with the stack
            // trace in this scenario as it will already be accounted for in the
            // Set.paginate call
            FQLInterpreter.StackTrace(Nil)
          ))
        .toQuery
  }

  /** evaluation: lazy */
  defMethod("map" -> tt.Set(tt.B))("mapper" -> tt.Function(Seq(tt.A), tt.B))(
    mapImpl(_, _, _))

  /** constructor for ProjectedSet from reification */
  defMethod("$map" -> tt.Set(tt.B))(
    "mapper" -> tt.Function(Seq(tt.A), tt.B),
    "ts" -> tt.Optional(tt.Time)) { (ctx, self, func, validTime) =>
    Result.guardM {
      checkArgArity("mapper", 1, func, ctx.stackTrace)
      ProjectedSet(
        self,
        func,
        ValueSet.timeFromValue(validTime),
        // when we are coming from reification, this means the 'set' has been
        // passed over the wire and we don't want to create it with the stack
        // trace in this scenario as it will already be accounted for in the
        // Set.paginate call
        FQLInterpreter.StackTrace(Nil)
      ).toQuery
    }
  }

  /** evaluation: eager */
  defMethod("lastWhere" -> tt.Optional(tt.A))("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, pred) =>
      lastWhereImpl(ctx, self, pred)
  }

  /** evaluation: lazy */
  defMethod("order" -> tt.Set(tt.A))
    .vararg("ordering" -> SetCompanion.orderingType) { (ctx, self, orderings) =>
      maybeEmitSetHint(ctx, self, "order", ctx.stackTrace.currentStackFrame).map {
        _ =>
          SetCompanion
            .collectOrderings(orderings, ctx.stackTrace)
            .map { OrderedSet(self, _, ctx.stackTrace) }
      }
    }

  /** evaluation: lazy */
  defMethod("reverse" -> tt.Set(tt.A))() { (_, self) =>
    self.reverse.toQuery
  }

  // materializing values/pages

  /** evaluation: eager */
  defMethod("paginate" -> SetCompanion.pageType(tt.A))() { (ctx, self) =>
    self.materializedPage(ctx, None).flatMapT(_.toValue(ctx).map(Result.Ok(_)))
  }

  /** evaluation: eager */
  defMethod("paginate" -> SetCompanion.pageType(tt.A))("size" -> tt.Int) {
    (ctx, self, size) =>
      checkSize(ctx, "size", size).flatMapT { _ =>
        self
          .materializedPage(ctx, Some(size.value))
          .flatMapT(_.toValue(ctx).map(Result.Ok(_)))
      }
  }

  defMethod("pageSize" -> selfType)("size" -> tt.Int) { (ctx, self, size) =>
    checkSize(ctx, "size", size).flatMapT { _ =>
      PagedSet(self, size.value, ctx.stackTrace).toQuery
    }
  }

  /** evaluation: eager */
  defMethod("any" -> tt.Boolean)("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) => self.any(ctx, predicate)
  }

  /** evaluation: eager */
  defMethod("first" -> tt.Optional(tt.A))() { (ctx, self) =>
    self.first(ctx)
  }

  /** evaluation: eager */
  defMethod("every" -> tt.Boolean)("predicate" -> tt.Predicate(tt.A)) {
    (ctx, self, predicate) =>
      maybeEmitSetHint(ctx, self, "every", ctx.stackTrace.currentStackFrame)
        .flatMap { _ =>
          self.every(ctx, predicate)
        }
  }

  /** evaluation: eager */
  defMethod("isEmpty" -> tt.Boolean)() { (ctx, self) =>
    self.isEmpty(ctx)
  }

  /** evaluation: eager */
  defMethod("includes" -> tt.Boolean)("element" -> tt.A) { (ctx, self, value) =>
    maybeEmitSetHint(ctx, self, "includes", ctx.stackTrace.currentStackFrame)
      .flatMap { _ =>
        self.includes(ctx, value)
      }
  }

  /** evaluation: eager, lazy for index sets as we just flip the index bounds. */
  defMethod("last" -> tt.Optional(tt.A))() { (ctx, self) =>
    self.reverse.first(ctx)
  }

  /** evaluation: lazy */
  defMethod("take" -> tt.Set(tt.A))("limit" -> tt.Int) { (ctx, self, limit) =>
    if (limit.value < 0) {
      QueryRuntimeFailure
        .InvalidBounds(
          ctx,
          "limit",
          s"greater than 0",
          limit,
          ctx.stackTrace
        )
        .map { err => Result.Err(err) }
    } else if (limit.value == 0) {
      ArraySet(Value.Array()).toQuery
    } else {
      TakeSet(self, limit.value).toQuery
    }
  }

  /** evaluation: eager */
  defMethod("nonEmpty" -> tt.Boolean)() { (ctx, self) =>
    self.nonEmpty(ctx)
  }

  /** evaluation: eager */
  defMethod("toArray" -> tt.Array(tt.A))() { (ctx, self) =>
    maybeEmitSetHint(ctx, self, "toArray", ctx.stackTrace.currentStackFrame)
      .flatMap { _ =>
        self.toArray(ctx)
      }
  }

  /** evaluation: lazy */
  defMethod("toSet" -> tt.Set(tt.A))() { (_, self) =>
    Result.Ok(self).toQuery
  }

  // whole-set iteration and aggregation

  /** evaluation: lazy */
  defMethod("concat" -> tt.Set(tt.Union(tt.A, tt.B)))("other" -> tt.Set(tt.B)) {
    (_, self, other) =>
      Result.Ok(self.concat(other)).toQuery
  }

  /** evaluation: eager */
  defMethod("fold" -> tt.B)(
    "seed" -> tt.B,
    "reducer" -> tt.Function(Seq(tt.B, tt.A), tt.B)) { (ctx, self, seed, reducer) =>
    maybeEmitSetHint(ctx, self, "fold", ctx.stackTrace.currentStackFrame).flatMap {
      _ =>
        Result.guardM {
          checkArgArity("reducer", 2, reducer, ctx.stackTrace)
          self.foldLeft(ctx, seed, reducer)
        }
    }
  }

  /** evaluation: eager */
  defMethod("foldRight" -> tt.B)(
    "seed" -> tt.B,
    "reducer" -> tt.Function(Seq(tt.B, tt.A), tt.B)) { (ctx, self, seed, reducer) =>
    maybeEmitSetHint(ctx, self, "foldRight", ctx.stackTrace.currentStackFrame)
      .flatMap { _ =>
        Result.guardM {
          checkArgArity("reducer", 2, reducer, ctx.stackTrace)
          self.foldRight(ctx, seed, reducer)
        }
      }
  }

  /** evaluation: eager */
  defMethod("forEach" -> tt.Null)("callback" -> tt.Function(Seq(tt.A), tt.Any)) {
    (ctx, self, func) =>
      maybeEmitSetHint(ctx, self, "forEach", ctx.stackTrace.currentStackFrame)
        .flatMap { _ =>
          Result.guardM {
            checkArgArity("func", 1, func, ctx.stackTrace)
            self.foreach(ctx, func)
          }
        }
  }

  /** evaluation: eager */
  defMethod("reduce" -> tt.Optional(tt.A))(
    "reducer" -> tt.Function(Seq(tt.A, tt.A), tt.A)) { (ctx, self, reducer) =>
    maybeEmitSetHint(ctx, self, "reduce", ctx.stackTrace.currentStackFrame).flatMap {
      _ =>
        Result.guardM {
          checkArgArity("reducer", 2, reducer, ctx.stackTrace)
          self.reduceLeft(ctx, reducer)
        }
    }
  }

  /** evaluation: eager */
  defMethod("reduceRight" -> tt.Optional(tt.A))(
    "reducer" -> tt.Function(Seq(tt.A, tt.A), tt.A)) { (ctx, self, reducer) =>
    maybeEmitSetHint(ctx, self, "reduceRight", ctx.stackTrace.currentStackFrame)
      .flatMap { _ =>
        Result.guardM {
          checkArgArity("reducer", 2, reducer, ctx.stackTrace)
          self.reduceRight(ctx, reducer)
        }
      }
  }

  /** evaluation: eager */
  defMethod("count" -> tt.Number)() { (ctx, self) =>
    maybeEmitSetHint(ctx, self, "count", ctx.stackTrace.currentStackFrame).flatMap {
      _ =>
        Result.guardM {
          val agg = self.foldLeft(ctx, Value.Int(0)) {
            case (Value.Int(i), _) => Result.Ok(Value.Int(i + 1)).toQuery
            case (v, _) =>
              Result.fail(
                QueryRuntimeFailure
                  .InvalidType(ValueType.NumberType, v, ctx.stackTrace))
          }

          agg flatMapT { v =>
            val n = v.asInstanceOf[Value.Int]
            Query.addCompute(n.value) map { _ => Result.Ok(n) }
          }
        }
    }
  }

  // event source conversion

  /** evaluation: pure */
  defMethod("toStream" -> tt.EventSource(tt.A))() { (ctx, self) =>
    self.toEventSource(ctx, ".toStream()")
  }

  /** evaluation: pure */
  defMethod("eventSource" -> tt.EventSource(tt.A))() { (ctx, self) =>
    self.toEventSource(ctx, ".eventSource()")
  }

  /** evaluation: pure */
  defMethod("changesOn" -> tt.EventSource(tt.A))
    .vararg("fields" -> tt.Function(Seq(tt.A), tt.Any)) { (ctx, self, fields) =>
      eventsOn(ctx, self, fields, ".changesOn()")
    }

  /** evaluation: pure */
  defMethod("eventsOn" -> tt.EventSource(tt.A))
    .vararg("fields" -> tt.Function(Seq(tt.A), tt.Any)) { (ctx, self, fields) =>
      eventsOn(ctx, self, fields, ".eventsOn()")
    }

  private def eventsOn(
    ctx: FQLInterpCtx,
    self: Value.Set,
    fields: IndexedSeq[Value.Func],
    method: String) =
    Result.guardM {
      val paths = fields.zipWithIndex map { case (field, i) =>
        field match {
          case Value.Lambda(ArraySeq(Some(Expr.This.name)), _, expr, _) =>
            ExprParser.processFieldPathExpr(Expr.ShortLambda(expr)) match {
              case (path, None) => new Path(path.toList)
              case (_, Some(_)) =>
                Result.fail(
                  QueryRuntimeFailure
                    .InvalidArgument(
                      i.toString,
                      "invalid field path given",
                      ctx.stackTrace
                    ))
            }
          case _ =>
            Result.fail(
              QueryRuntimeFailure
                .InvalidArgument(
                  i.toString,
                  "invalid field path given",
                  ctx.stackTrace
                ))
        }
      }
      self.changesOn(ctx, paths, method)
    }
}

object SetCompanion extends CompanionObject("Set") {

  def contains(v: Value): Boolean = v match {
    case _: Value.Set => true
    case _            => false
  }

  val accessorType = tt.Function(Seq(tt.A), tt.Any)
  val ascType = tt.Struct("asc" -> accessorType)
  val descType = tt.Struct("desc" -> accessorType)
  val orderingType = tt.Union(ascType, descType, accessorType)

  def pageType(elem: TypeTag[_]) =
    tt.Struct("data" -> tt.Array(elem.staticType), "after" -> tt.Optional(tt.Str))

  // TODO: Build possible prefixes as unions. For example, a range boundary for an
  // index with 2 covered values should be constraint to:
  //
  // Scalar | (Scalar, Scalar) | (Scalar, Scalar, Doc)
  //
  // FIXME: We should not need a union here in order to make fields optional. The
  // type `tt.Struct("from" -> tt.Optional(tt.Any), "to" -> tt.Optional(tt.Any))`
  // should be equivant to the union below.
  val rangeType =
    tt.Union(
      tt.Struct("from" -> tt.Any),
      tt.Struct("to" -> tt.Any),
      tt.Struct("from" -> tt.Any, "to" -> tt.Any)
    )

  private[stdlib] def collectOrderings(
    orderings: Seq[Value],
    stackTrace: FQLInterpreter.StackTrace)
    : Result[IndexedSeq[OrderedSet.Ordering]] = {
    import OrderedSet.{ Asc, Desc }

    Result.guard {
      def invalidOrdArity(idx: Int, f: Value.Func) =
        Result.fail(
          QueryRuntimeFailure
            .InvalidFuncParamArity(s"ordering$idx", 1, f.arity, stackTrace))

      def invalidOrdArg(idx: Int, v: Value) =
        Result.fail(
          QueryRuntimeFailure
            .InvalidArgumentType(
              s"ordering$idx",
              ValueType.AnyFunctionType,
              v.dynamicType,
              stackTrace))

      val ords = orderings.iterator.zipWithIndex.map {
        case (f: Value.Func, idx) =>
          if (f.arity.accepts(1)) Asc(f) else invalidOrdArity(idx, f)

        case (s: Value.Struct.Full, idx) if s.fields.sizeIs == 1 =>
          def asc = s.fields.get("asc").collect { case f: Value.Func =>
            if (f.arity.accepts(1)) Asc(f) else invalidOrdArity(idx, f)
          }
          def desc = s.fields.get("desc").collect { case f: Value.Func =>
            if (f.arity.accepts(1)) Desc(f) else invalidOrdArity(idx, f)
          }

          asc.orElse(desc).getOrElse(invalidOrdArg(idx, s))

        case (v, idx) => invalidOrdArg(idx, v)
      }.toIndexedSeq

      Result.Ok(ords)
    }
  }

  defStaticFunction("single" -> tt.Set(tt.A))("element" -> tt.A) { case (_, value) =>
    Result.Ok(SingletonSet(value)).toQuery
  }

  defStaticFunction("sequence" -> tt.Set(tt.Number))(
    "from" -> tt.Int,
    "until" -> tt.Int) { (_, from, until) =>
    SequenceSet(from.value, until.value).toQuery
  }

  private[stdlib] def paginateImpl(
    ctx: FQLInterpCtx,
    cursor: String,
    sizeOpt: Option[Int]
  ): Query[Result[Value.Struct]] =
    Value.SetCursor.fromBase64(cursor) match {
      case None         => QueryRuntimeFailure.InvalidCursor(ctx.stackTrace).toQuery
      case Some(cursor) => paginateImpl(ctx, cursor, sizeOpt)
    }

  private[stdlib] def paginateImpl(
    ctx: FQLInterpCtx,
    cursor: Value.SetCursor,
    sizeOpt: Option[Int]
  ): Query[Result[Value.Struct]] =
    ctx.withSystemValidTime(cursor.systemValidTS) flatMap { ctx =>
      ctx.evalWithTypecheck(
        cursor.set,
        ValueReification.vctx(cursor.values),
        // Do not typecheck cursors, because they will have already been checked
        // as part of the original query.
        typeMode = FQLInterpreter.TypeMode.Disabled
      ) flatMap {
        case Result.Ok((set: Value.Set, _)) =>
          val size = sizeOpt.getOrElse(cursor.pageSize)

          val cur = cursor.ords match {
            case Some(ords) =>
              ValueSet.Continuation(ords.map(Value.fromIR(_, Span.Null)))
            case None => ValueSet.InitialPage
          }
          set
            .paginate(ctx, size, cur)
            .flatMapT(_.toValue(ctx).map(Result.Ok(_)))
        case Result.Ok(_) =>
          QueryRuntimeFailure.InvalidCursor(ctx.stackTrace).toQuery
        case res @ Result.Err(_) =>
          Query.value(res)
      }
    }

  // The return page type must be any, because we cannot derive it from arguments.
  defStaticFunction("paginate" -> pageType(tt.Any))(
    "cursor" -> tt.Union(tt.Str, tt.SetCursor)) {
    case (ctx, Value.Str(cur))       => paginateImpl(ctx, cur, None)
    case (ctx, cur: Value.SetCursor) => paginateImpl(ctx, cur, None)
    case (ctx: FQLInterpCtx, cur) =>
      QueryRuntimeFailure
        .InvalidArgumentType(
          "cursor",
          ValueType.StringType,
          cur.dynamicType,
          ctx.stackTrace)
        .toQuery
  }

  defStaticFunction("paginate" -> pageType(tt.Any))(
    "cursor" -> tt.Union(tt.Str, tt.SetCursor),
    "size" -> tt.Int) { case (ctx, cur, size) =>
    Result.guardM {
      SetPrototype.checkSize(ctx, "size", size).flatMapT { _ =>
        cur match {
          case Value.Str(cur) =>
            paginateImpl(ctx, cur, Some(size.value))
          case cur: Value.SetCursor =>
            paginateImpl(ctx, cur, Some(size.value))
          case _ =>
            QueryRuntimeFailure
              .InvalidArgumentType(
                "cursor",
                ValueType.StringType,
                cur.dynamicType,
                ctx.stackTrace)
              .toQuery
        }
      }
    }
  }
}
