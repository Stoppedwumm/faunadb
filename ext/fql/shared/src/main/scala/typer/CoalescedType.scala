package fql.typer

import fql.ast.{ Literal, Name, Span, TypeExpr }
import scala.collection.immutable.{ ArraySeq, ListSet, SeqMap }
import scala.collection.mutable.{ Builder, Map => MMap, Set => MSet }
import scala.util.chaining._

/** A CoalescedType is a flattened structure which represents the union (in a
  * positive/value context) or intersection (negative/use context) of its
  * constituents. Essentially, it is a more convenient data structure for the
  * simplification algorithms than the Constraint or Type values.
  *
  * It contains an inner CT structure (the real star of the show) and a Map of
  * associated recursive variables.
  */
case class CoalescedType(
  ct: CoalescedType.CT,
  recVars: MMap[Type.Var, CoalescedType.CT],
  aliases: Map[(String, Boolean), CoalescedType.CT])
    extends CoalescedType.ToExpr

object CoalescedType {

// TODO: it may make sense to split this out into Use and Value types as well. See Constraints
  final case class CT(
    isAny: Boolean = false,
    isNever: Boolean = false,
    poisoned: Boolean = false,
    vars: ListSet[Type.Var] = ListSet.empty,
    overloads: SeqMap[Alt, CT] = SeqMap.empty,
    singles: ListSet[Literal] = ListSet.empty,
    ids: SeqMap[String, Boolean] = SeqMap.empty,
    // if this is an alias, the instantiated alias is in the Option
    constructs: SeqMap[String, ArraySeq[CT]] = SeqMap.empty,
    func: SeqMap[
      (Int, Boolean),
      (ArraySeq[(Option[String], CT)], Option[(Option[String], CT)], CT)] =
      SeqMap.empty,
    access: Option[(ArraySeq[CT], Option[CT], CT)] = None,
    proj: Option[(CT, CT)] = None,
    record: Option[(SeqMap[String, CT], Option[CT])] = None,
    interface: Option[SeqMap[String, CT]] = None,
    tuples: SeqMap[Int, ArraySeq[CT]] = SeqMap.empty,
    diffs: ListSet[(CT, CT)] = ListSet.empty
  ) {

    /** merge with another CT, in either a pos/value context or neg/use context */
    def merge(o: CT, pol: Boolean): CT = {
      var ids0 = ids ++ o.ids
      var singles0 = singles ++ o.singles

      if (pol) {
        // Special-case handling of true | false enum, aka. boolean
        var seenTrue = false
        var seenFalse = false

        // singletons get subsumed by their primitive types in unions
        singles0 = singles0
          .filterNot { l =>
            if (l == Literal.True) seenTrue = true
            if (l == Literal.False) seenFalse = true
            ids0.exists { case (n, skol) =>
              !skol && Type.isValueOf(l, Type.Named(n))
            }
          }

        if (seenTrue && seenFalse) {
          ids0 += (Type.Boolean.name.str -> false)
          singles0 -= Literal.True
          singles0 -= Literal.False
        }
      } else {
        // the intersection of singletons and any other type is only singletons
        ids0 = if (singles0.isEmpty) ids0 else SeqMap.empty
      }

      CT(
        isAny = if (pol) isAny || o.isAny else isAny && o.isAny,
        isNever = if (pol) isNever && o.isNever else isNever || o.isNever,
        poisoned = if (pol) poisoned && o.poisoned else poisoned || o.poisoned,
        vars = vars ++ o.vars,
        overloads = overloads ++ o.overloads,
        singles = singles0,
        ids = ids0,
        constructs = mergeMap(constructs, o.constructs) { (_, args0, args1) =>
          val args =
            args0.iterator.zip(args1).map { case (a0, a1) => a0.merge(a1, pol) }
          args.to(ArraySeq)
        },
        func =
          // merging is only possible when the arity and varidic is the same. Any
          // other merges lose information.
          //
          // examples:
          // (int)         => x | (str)         => x  -->  (int | str) => x
          // (int, int)    => x | (str)         => x  -->  <can't merge>
          // (...int)      => x | (str)         => x  -->  <can't merge>
          // (...int)      => x | (...str)      => x  --> (...int | str) => x
          // (int) => x         | (str, ...str) => x  --> <can't merge>

          mergeMap(func, o.func) { case (_, (ps0, v0, r0), (ps1, v1, r1)) =>
            val ps = (0 until (ps0.length max ps1.length)).flatMap { i =>
              val p0 = ps0.lift(i).orElse(v0)
              val p1 = ps1.lift(i).orElse(v1)
              // Arbitrarily choose the name of p0
              (p0, p1) match {
                case (Some(p0), Some(p1)) => Some(p0._1 -> p0._2.merge(p1._2, !pol))
                case _                    => None
              }
            }
            val v = (v0, v1) match {
              case (Some(v0), Some(v1)) => Some(v0._1 -> v0._2.merge(v1._2, !pol))
              case _                    => None
            }
            val r = r0.merge(r1, pol)
            (ps.to(ArraySeq), v, r)
          },
        access = mergeOpt(access, o.access) { case ((ps0, v0, r0), (ps1, v1, r1)) =>
          val ps = (0 until (ps0.length max ps1.length)).flatMap { i =>
            val p0 = ps0.lift(i).orElse(v0)
            val p1 = ps1.lift(i).orElse(v1)
            (p0, p1) match {
              case (Some(p0), Some(p1)) => Some(p0.merge(p1, !pol))
              case _                    => None
            }
          }
          val v = (v0, v1) match {
            case (Some(v0), Some(v1)) => Some(v0.merge(v1, !pol))
            case _                    => None
          }
          val r = r0.merge(r1, pol)
          (ps.to(ArraySeq), v, r)
        },
        proj = mergeOpt(proj, o.proj) { case ((p0, r0), (p1, r1)) =>
          (p0.merge(p1, !pol), r0.merge(r1, pol))
        },
        record = mergeOpt(record, o.record) { case ((fs0, w0), (fs1, w1)) =>
          val fs = SeqMap.newBuilder[String, CT]

          fs0.foreach { case (l, v0) =>
            // if l is common, merge with fs1(l), else merge with w1
            val v = fs1.get(l).orElse(w1).map(v1 => l -> v0.merge(v1, pol))
            // otherwise union vs inter depends on polarity
            fs ++= v.orElse(Option.when(!pol)(l -> v0))
          }

          // merge fs1 remainder
          fs1.foreach { case (l, v1) =>
            if (!fs0.contains(l)) {
              val v = w0.map(v0 => l -> v0.merge(v1, pol))
              fs ++= v.orElse(Option.when(!pol)(l -> v1))
            }
          }

          val w = (w0, w1) match {
            case (Some(w0), Some(w1)) => Some(w0.merge(w1, pol))
            case (w0, w1)             => if (pol) None else w0.orElse(w1)
          }
          (fs.result(), w)
        },
        interface = mergeOpt(interface, o.interface) { case (fs0, fs1) =>
          val fs = SeqMap.newBuilder[String, CT]

          fs0.foreach { case (l, v0) =>
            // if l is common, merge with fs1(l), else merge with w1
            val v = fs1.get(l).map(v1 => l -> v0.merge(v1, pol))
            // otherwise union vs inter depends on polarity
            fs ++= v.orElse(Option.when(!pol)(l -> v0))
          }

          // merge fs1 remainder
          fs1.foreach { case (l, v1) =>
            if (!fs0.contains(l)) {
              fs ++= Option.when(!pol)(l -> v1)
            }
          }

          fs.result()
        },
        tuples = mergeMap(tuples, o.tuples) { (_, es0, es1) =>
          val es = es0.iterator.zip(es1).map { case (e0, e1) => e0.merge(e1, pol) }
          es.to(ArraySeq)
        },
        diffs = diffs ++ o.diffs
      )
    }

    private def mergeOpt[T](a: Option[T], b: Option[T])(f: (T, T) => T): Option[T] =
      (a, b) match {
        case (Some(a), Some(b)) => Some(f(a, b))
        case (a, b)             => a.orElse(b)
      }

    private def mergeMap[K, V](a: SeqMap[K, V], b: SeqMap[K, V])(
      f: (K, V, V) => V): SeqMap[K, V] = {
      val rv = SeqMap.newBuilder[K, V]

      a.foreach { case (k, va) =>
        b.lift(k) match {
          case Some(vb) => rv += k -> f(k, va, vb)
          case None     => rv += k -> va
        }
      }

      b.foreach { case (k, vb) =>
        if (!a.contains(k)) {
          rv += k -> vb
        }
      }

      rv.result()
    }
  }

  sealed trait ToExpr { self: CoalescedType =>
    def toValueExpr(typeShapes: Typer.ShapeCtx): TypeExpr =
      toTypeSchemeExpr(typeShapes, true).expr

    def toUseExpr(typeShapes: Typer.ShapeCtx): TypeExpr =
      toTypeSchemeExpr(typeShapes, false).expr

    /** Generate a user-presentable TypeExpr from an internal CoalescedTypeScheme. Takes a
      * set of existing type names in order to avoid colliding with them
      * generating type parameter names.
      */
    def toTypeSchemeExpr(
      typeShapes: Typer.ShapeCtx = Map.empty,
      pol: Boolean = true): TypeExpr.Scheme = {
      type CTorV = Either[CT, Type.Var]
      val recVarExprs = MMap.empty[CTorV, TypeExpr.Id]
      val aliasDefs = MMap.empty[(TypeExpr, Boolean), TypeExpr]
      val tparamExprs = MMap.empty[Type.Var, TypeExpr.Id]
      val tparams = MSet.empty[String]
      var varNamesUsed = 0

      // used to determine if an ID is a generic param or not for difference gen
      val paramVars = MSet.empty[TypeExpr]

      def genVarExpr(): TypeExpr.Id = {
        val letter = Simplifier.Letters(varNamesUsed % Simplifier.Letters.size)
        val repeat = (varNamesUsed / Simplifier.Letters.size) + 1
        val name = letter * repeat
        varNamesUsed += 1

        if (typeShapes.contains(name)) {
          genVarExpr()
        } else {
          val id = TypeExpr.Id(name, Span.Null)
          paramVars += id
          id
        }
      }

      def processAliasDef(name: String, pol: Boolean, te: TypeExpr): Unit =
        aliases.get((name, pol)).foreach { al =>
          aliasDefs.get((te, pol)) match {
            case None =>
              // Set the alias def to the nominal type before rendering its
              // definition, so that if the definition is recursive, the nominal
              // type is used as the base case. We do it this way instead of
              // through the pending cache in expr0 in order to not generate a
              // recursive type expr.
              aliasDefs.update((te, pol), te)
              aliasDefs.update((te, pol), expr0(Left(al), pol)(Set.empty))
            case Some(_) =>
          }
        }

      def expr0(ty: CTorV, pol: Boolean)(
        implicit pending: Set[(CTorV, Boolean)]): TypeExpr =
        if (pending.contains((ty, pol))) {
          recVarExprs.getOrElseUpdate(ty, genVarExpr())
        } else {
          val res = (pending + ((ty, pol))).pipe { implicit pending =>
            ty match {
              case Right(v) =>
                recVars.get(v) match {
                  case None     => tparamExprs.getOrElseUpdate(v, genVarExpr())
                  case Some(ct) => expr0(Left(ct), pol)
                }
              case Left(ct) =>
                val b = Seq.newBuilder[TypeExpr]

                def consolidate(tpes: Seq[TypeExpr]): TypeExpr = {
                  // HACK: put null last. would be nice to have a stable
                  // TypeExpr sort as we currently rely on the vagaries of Set
                  // order.
                  val tseq = {
                    var ntpe = Option.empty[TypeExpr]
                    val b = Seq.newBuilder[TypeExpr]
                    tpes.foreach {
                      case t @ TypeExpr.Id(Type.Null.name.str, _) => ntpe = Some(t)
                      case t                                      => b += t
                    }
                    b ++= ntpe
                    b.result().distinct
                  }

                  def overload(ms: Seq[TypeExpr]) =
                    ms match {
                      case Seq(m)    => m
                      case ms if pol => TypeExpr.Union(ms, Span.Null)
                      case ms        => TypeExpr.Intersect(ms, Span.Null)
                    }

                  // FIXME: does it really make sense to use Any as our Top
                  // value here? This may be unsound if Any is also the dynamic
                  // escape hatch.
                  tseq match {
                    case _ if ct.poisoned =>
                      if (pol) TypeExpr.Hole(Span.Null) else TypeExpr.Any(Span.Null)

                    case ms if ct.isAny =>
                      // mix in Any if in value pos. drop it if in use pos.
                      def any = TypeExpr.Any(Span.Null)
                      val ms0 = if (ms.isEmpty || pol) any +: ms else ms
                      overload(ms0)

                    case ms if ct.isNever =>
                      // Never subsumes other types in use pos, is dropped in value
                      // pos.
                      def never = TypeExpr.Never(Span.Null)
                      val ms0 = if (ms.isEmpty || !pol) Seq(never) else ms
                      overload(ms0)

                    case Seq() if pol  => TypeExpr.Never(Span.Null)
                    case Seq() if !pol => TypeExpr.Any(Span.Null)
                    case ms            => overload(ms)
                  }
                }

                ct.vars.foreach { v => b += expr0(Right(v), pol) }

                if (ct.overloads.nonEmpty) {
                  val seen = MSet.empty[TypeExpr]
                  val variants = Seq.newBuilder[TypeExpr]

                  ct.overloads.foreach { case (_, ct) =>
                    val te = expr0(Left(ct), pol)
                    if (seen.add(te)) {
                      variants += te
                    }
                  }

                  if (pol) {
                    b += TypeExpr.Intersect(variants.result(), Span.Null)
                  } else {
                    b += TypeExpr.Union(variants.result(), Span.Null)
                  }
                }

                ct.singles.foreach { l => b += TypeExpr.Singleton(l, Span.Null) }

                ct.ids.foreach { case (n, skol) =>
                  val te = TypeExpr.Id(n, Span.Null)

                  if (skol) {
                    tparams += n
                  } else {
                    processAliasDef(n, pol, te)
                  }
                  b += te
                }
                ct.constructs.foreach { case (n, args) =>
                  val args0 = args.map(a => expr0(Left(a), pol))

                  val te =
                    if (args0.isEmpty) {
                      TypeExpr.Id(n, Span.Null)
                    } else {
                      TypeExpr.Cons(Name(n, Span.Null), args0, Span.Null)
                    }

                  processAliasDef(n, pol, te)
                  b += te
                }
                ct.func.foreach { case (_, (ps, v, r)) =>
                  val params = ps.map { case (name, p) =>
                    name.map(Name(_, Span.Null)) -> expr0(Left(p), !pol)
                  }
                  val variadic = v.map { case (name, v) =>
                    name.map(Name(_, Span.Null)) -> expr0(Left(v), !pol)
                  }
                  val ret = expr0(Left(r), pol)
                  b += TypeExpr.Lambda(params, variadic, ret, Span.Null)
                }
                ct.access.foreach { case (ps, v, r) =>
                  val params = ps.map { p => None -> expr0(Left(p), !pol) }
                  val variadic = v.map { v => None -> expr0(Left(v), !pol) }
                  val ret = expr0(Left(r), pol)
                  // TODO: Add TypeExpr.Access
                  b += TypeExpr.Lambda(params, variadic, ret, Span.Null)
                }
                ct.proj.foreach { case (p, r) =>
                  b += TypeExpr.Projection(
                    expr0(Left(p), !pol),
                    expr0(Left(r), pol),
                    Span.Null)
                }
                ct.record.foreach { case (fs, w) =>
                  val elems = fs.iterator.map { case (l, e) =>
                    (Name(l, Span.Null), expr0(Left(e), pol))
                  }.toSeq
                  val wild = w.map(w => expr0(Left(w), pol))
                  b += TypeExpr.Object(elems, wild, Span.Null)
                }
                ct.interface.foreach { fs =>
                  val elems = fs.iterator.map { case (l, e) =>
                    (Name(l, Span.Null), expr0(Left(e), pol))
                  }.toSeq
                  b += TypeExpr.Interface(elems, Span.Null)
                }
                ct.tuples.foreach { case (_, es) =>
                  val elems = es.map { e => expr0(Left(e), pol) }
                  b += TypeExpr.Tuple(elems, Span.Null)
                }
                ct.diffs.foreach { case (i, sub) =>
                  def elems(te: TypeExpr) = te match {
                    case TypeExpr.Union(ms, _) if pol      => ms
                    case TypeExpr.Intersect(ms, _) if !pol => ms
                    case m                                 => Seq(m)
                  }

                  val members = elems(expr0(Left(i), pol))
                  val diff = expr0(Left(sub), pol)
                  val diffSet = elems(diff).toSet

                  def shouldSubtract(te: TypeExpr): Boolean = {
                    if (diffSet.isEmpty) return false

                    // in the overload case, if _any_ match, the whole thing goes.
                    te match {
                      case TypeExpr.Union(ms, _) if ms.exists(shouldSubtract) =>
                        true
                      case TypeExpr.Intersect(ms, _) if ms.exists(shouldSubtract) =>
                        true
                      case _ =>
                        diffSet.contains(te)
                    }
                  }

                  var hasParam = false
                  val b0 = Seq.newBuilder[TypeExpr]

                  def go(m: TypeExpr, b: Builder[TypeExpr, Seq[TypeExpr]]): Unit = {
                    if (shouldSubtract(m)) return ()

                    aliasDefs.get(m -> pol) match {
                      case None =>
                        if (paramVars.contains(m)) hasParam = true
                        b += m

                      case Some(alias) =>
                        val b0 = Seq.newBuilder[TypeExpr]
                        val els = elems(alias)
                        els.foreach(go(_, b0))

                        if (els == b0.result()) {
                          b += m
                        } else {
                          b ++= b0.result()
                        }
                    }
                  }

                  members.foreach(go(_, b0))

                  b ++= (consolidate(b0.result()) match {
                    case _: TypeExpr.Never => None
                    case te if !hasParam   => Some(te)
                    case te => Some(TypeExpr.Difference(te, diff, Span.Null))
                  })
                }

                consolidate(b.result())
            }
          }

          recVarExprs.get(ty) match {
            case Some(id) => TypeExpr.Recursive(id.name, res, Span.Null)
            case None     => res
          }
        }

      val res = expr0(Left(ct), pol)(Set.empty)
      tparams ++= tparamExprs.values.iterator.map(_.str)

      TypeExpr.Scheme(tparams.toSeq.sorted, res)
    }
  }
}
