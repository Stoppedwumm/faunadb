package fql.typer

import fql.ast.{ Name, Span, TypeExpr }
import fql.ast.display._
import scala.collection.immutable.{ ArraySeq, ListSet, SeqMap }
import scala.collection.mutable.{ Map => MMap, Set => MSet, Stack }
import scala.util.chaining._

object Simplifier {
  val Letters = ('A' to 'Z').map(_.toString).toIndexedSeq

}

trait TyperSimplifier { self: Typer =>
  import CoalescedType.CT

  def toTypeSchemeExpr(sch: TypeScheme): TypeExpr.Scheme =
    simplifyValue(sch.raw).pipe(_.toTypeSchemeExpr(typeShapes))

  def valueToExpr(ty: Constraint.Value): TypeExpr =
    simplifyValue(ty).pipe(_.toValueExpr(typeShapes))

  def useToExpr(ty: Constraint.Use): TypeExpr =
    simplifyUse(ty).pipe(_.toUseExpr(typeShapes))

  def simplifyValue(ty: Constraint.Value): CoalescedType =
    canonicalCT(ty, true).pipe(simplifyCT(_, true))

  def simplifyUse(ty: Constraint.Use): CoalescedType =
    canonicalCT(ty, false).pipe(simplifyCT(_, false))

  def displayValue(v: Constraint.Value): String =
    valueToExpr(v).display

  def displayUse(u: Constraint.Use): String =
    useToExpr(u).display

  /** See simple-sub's `canonicalizeType`. This function generates a CoalescedType
    * for the provided constraint. This is accomplished in two steps:
    *
    *  - The type is converted to a CoalescedType. Its transitive variable
    *    bounds are also flattened into a single set. (see toCT below)
    *
    *  - Concrete bounds and co-occurring recursive types are merged. With
    *    merged recursive types having a cycle length equal to the LCD of each
    *    rectype's original cycle depth.
    */
  def canonicalCT(ty: Constraint, pol: Boolean): CoalescedType = {
    type Polar = (CT, Boolean)
    val recCTs = MMap.empty[Type.Var, CT]
    val aliasDefs = MMap.empty[(String, Boolean), CT]
    val rec = MMap.empty[Polar, Type.Var]

    def mergeBounds(ct: CT, pol: Boolean)(
      implicit pending: Set[Polar] = Set.empty): CT = {
      val pt = (ct, pol)
      if (pending.contains(pt)) {
        // Recursive type
        CT(vars =
          ListSet(rec.getOrElseUpdate(pt, freshVar(ty.span)(Type.Level.Zero))))
      } else {
        val res = ct.vars.iterator
          .flatMap { v =>
            val bounds = if (pol) v.values else v.uses
            bounds.iterator.flatMap {
              // FIXME: properly filter based on alts?
              case (_: Type.Var, _) => None
              case (ty, _)          => Some(toCT(ty, pol, aliasDefs))
              case _                => None
            }
          }
          .fold(ct)(_.merge(_, pol))

        val adapted = (pending + pt).pipe { implicit pending =>
          res.copy(
            overloads = res.overloads.map { case (a, v) =>
              a -> mergeBounds(v, pol)(pending)
            },
            constructs = res.constructs.map { case (n, ags) =>
              (n, ags.map(mergeBounds(_, pol)))
            },
            func = res.func.map { case (ar, (ps, v, r)) =>
              val func = (
                ps.map(p => p._1 -> mergeBounds(p._2, !pol)),
                v.map(v => v._1 -> mergeBounds(v._2, !pol)),
                mergeBounds(r, pol))
              (ar, func)
            },
            access = res.access.map { case (ps, v, r) =>
              (
                ps.map(mergeBounds(_, !pol)),
                v.map(mergeBounds(_, !pol)),
                mergeBounds(r, pol))
            },
            proj = res.proj.map { case (p, r) =>
              (mergeBounds(p, !pol), mergeBounds(r, pol))
            },
            record = res.record.map { case (fs, w) =>
              (
                fs.map { case (l, v) => (l, mergeBounds(v, pol)) },
                w.map(mergeBounds(_, pol)))
            },
            interface = res.interface.map { fs =>
              fs.map { case (l, v) => (l, mergeBounds(v, pol)) }
            },
            tuples = res.tuples.map { case (i, es) =>
              (i, es.map(mergeBounds(_, pol)))
            },
            diffs = res.diffs.map { case (v, sub) =>
              (mergeBounds(v, pol), mergeBounds(sub, pol))
            }
          )
        }

        rec.get(pt) match {
          case Some(v) =>
            recCTs += ((v, adapted))
            CT(vars = ListSet(v))
          case None => adapted
        }
      }
    }

    val ct = toCT(ty, pol, aliasDefs)
    val ct0 = mergeBounds(ct, pol)
    val alias0 = aliasDefs.view.mapValues(ct => mergeBounds(ct, pol)).toMap

    CoalescedType(ct0, recCTs, alias0)
  }

  /** Generate a CoalescedType.CT from a Constraint or Type */
  private def toCT(
    ty: Constraint,
    pol: Boolean,
    aliases: MMap[(String, Boolean), CT]): CT = {
    def fromRecord(
      fields: SeqMap[String, Constraint],
      wild: Option[Constraint],
      pol: Boolean) = {
      val fs = fields.iterator.map { case (l, ty) => (l, toCT(ty, pol, aliases)) }
      val w = wild.map(toCT(_, pol, aliases))
      CT(record = Some((fs.to(SeqMap), w)))
    }

    def fromInterface(field: String, ret: Constraint, pol: Boolean) =
      CT(interface = Some(SeqMap(field -> toCT(ret, pol, aliases))))

    def fromTupleElems(elems: ArraySeq[Constraint], pol: Boolean) = {
      val tup = elems.iterator.map(toCT(_, pol, aliases)).to(ArraySeq)
      CT(tuples = SeqMap(elems.length -> tup))
    }

    def fromFunc(
      ps: Seq[(Option[Name], Constraint)],
      v: Option[(Option[Name], Constraint)],
      r: Constraint,
      pol: Boolean) = {
      val params =
        ps.iterator
          .map { case (name, arg) => name.map(_.str) -> toCT(arg, !pol, aliases) }
          .to(ArraySeq)
      val variadic = v.map { case (name, arg) =>
        name.map(_.str) -> toCT(arg, pol, aliases)
      }
      val ret = toCT(r, pol, aliases)
      CT(func = SeqMap((params.size, variadic.isDefined) -> (params, variadic, ret)))
    }

    def fromAccess(
      ps: Seq[Constraint],
      v: Option[Constraint],
      r: Constraint,
      pol: Boolean) = {
      val params = ps.iterator.map(toCT(_, !pol, aliases)).to(ArraySeq)
      val variadic = v.map(toCT(_, pol, aliases))
      val ret = toCT(r, pol, aliases)
      CT(access = Some((params, variadic, ret)))
    }

    def fromOverload(variants: Seq[(Alt, Constraint)], pol: Boolean) =
      CT(overloads = variants.iterator
        .map { case (a, v) => a -> toCT(v, pol, aliases) }
        .to(SeqMap))

    def fromOverloadTy(variants: Seq[Constraint], inter: Boolean, pol: Boolean) =
      if (inter && pol || !inter && !pol) {
        val grp = freshAltGrp(variants.size, false, rootAlt, Type.Level.Zero)
        val overloads =
          variants.iterator.zip(grp.childAlts).map { case (v, a) =>
            a -> toCT(v, pol, aliases)
          }
        CT(overloads = overloads.to(SeqMap))
      } else {
        variants.iterator.map(toCT(_, pol, aliases)).reduceLeft(_.merge(_, pol))
      }

    ty match {
      case Constraint.Lazy(v) => toCT(v, pol, aliases)

      case l: Constraint.Lit => CT(singles = ListSet(l.lit))
      case t: Type.Singleton => CT(singles = ListSet(t.lit))

      case u: Constraint.Interface => fromInterface(u.field, u.ret, pol)
      case t: Type.Interface       => fromInterface(t.field, t.ret, pol)

      case v: Constraint.Rec => fromRecord(v.fields, None, pol)
      case t: Type.Record    => fromRecord(t.fields, t.wildcard, pol)

      case v: Constraint.Tup => fromTupleElems(v.elems, pol)
      case t: Type.Tuple     => fromTupleElems(t.elems, pol)

      case u: Constraint.Proj =>
        CT(proj = Some((toCT(u.proj, !pol, aliases), toCT(u.ret, pol, aliases))))

      case v: Constraint.Func  => fromFunc(v.params, v.variadic, v.ret, pol)
      case u: Constraint.Apply => fromFunc(u.args.map(None -> _), None, u.ret, pol)
      case t: Type.Function    => fromFunc(t.params, t.variadic, t.ret, pol)

      case u: Constraint.Access => fromAccess(u.args, None, u.ret, pol)

      case v: Constraint.Intersect => fromOverload(v.variants, pol)
      case t: Type.Intersect       => fromOverloadTy(t.variants, true, pol)

      case u: Constraint.Union => fromOverload(u.variants, pol)
      case t: Type.Union       => fromOverloadTy(t.variants, false, pol)

      case v: Constraint.Diff =>
        CT(diffs = ListSet((toCT(v.value, pol, aliases), toCT(v.sub, pol, aliases))))
      case u: Constraint.DiffGuard =>
        CT(diffs = ListSet((toCT(u.use, pol, aliases), toCT(u.sub, pol, aliases))))

      case _: Type.Any   => if (!pol) CT() else CT(isAny = true)
      case _: Type.Top   => if (!pol) CT() else CT(isAny = true)
      case _: Type.Never => if (pol) CT() else CT(isNever = true)

      case Type.Skolem(name, _) => CT(ids = SeqMap(name.str -> true))

      case Type.Named(name, _, args) =>
        val shape = typeShape(name)
        val alias = shape.alias

        alias.foreach { al =>
          if (!aliases.contains((name.str, pol))) {
            // stop general recursion
            aliases((name.str, pol)) = CT()
            val pairs = shape.tparams.zip(args)
            val defn = if (pol) {
              toCT(instantiateValue(al, pairs)(Type.Level.Zero), pol, aliases)
            } else {
              toCT(instantiateUse(al, pairs)(Type.Level.Zero), pol, aliases)
            }
            aliases((name.str, pol)) = defn
          }
        }

        if (args.isEmpty) {
          CT(ids = SeqMap(name.str -> false))
        } else {
          val argCTs = args.iterator.map(toCT(_, pol, aliases)).to(ArraySeq)
          CT(constructs = SeqMap(name.str -> argCTs))
        }

      case v: Type.Var =>
        var poisoned = false
        // when encountering a variable, we flatten out transitive variable bounds
        var vars = ListSet.empty[Type.Var]
        val stack = Stack(v)
        while (!stack.isEmpty) {
          val v = stack.pop()
          if (!vars.contains(v)) {
            vars += v
            poisoned = poisoned || v.poisoned
            val bounds = if (pol) v.values else v.uses
            // FIXME: properly filter based on alts
            stack.pushAll(bounds.iterator.collect { case (v: Type.Var, _) =>
              v
            })
          }
        }
        CT(poisoned = poisoned, vars = vars)
    }
  }

  /** See simple-sub's simplifyType. This function performs type variable
    * elimination/unification based on co-occurrence analysis. Variables are
    * eliminated using three strategies:
    *
    *  1. If a var `a` always occurs positively or negatively with var `b` and
    *     vice versa, they are indistinguishable and may be unified.
    *
    *     Examples: (a & b) -> (a, b) === a -> (a, a)
    *               a -> b -> a | b === a -> a -> a
    *
    *  2. If a variable occurs positively and negatively with some other type, it
    *     is redundant with the other type and may be eliminated.
    *
    *     Example: (a & Int) -> (a | Int) === Int -> Int
    *
    *  3. If a variable occurs only positively or negatively, it is eliminated.
    *     Conceptually this is the same as the second strategy, where the other
    *     type is Never (ie Bottom).
    *
    *     Example: (a & Int) -> Int === Int -> Int
    *              Int -> (a | Int) === Int -> Int
    */
  def simplifyCT(cts: CoalescedType, pol: Boolean): CoalescedType = {
    val ct = cts.ct
    val recs = cts.recVars.clone() // make a shallow copy which we will mutate.
    val aliases = cts.aliases

    val allVars = MSet.empty[Type.Var]
    val coOccs = MMap.empty[(Type.Var, Boolean), Set[Type]]

    def occurrences(ct: CT) = {
      val b = Set.newBuilder[Type]
      if (ct.isAny) b += Type.Any
      if (ct.isNever) b += Type.Never
      b ++= ct.vars
      b ++= ct.singles.iterator.map(Type.Singleton(_, Span.Null))
      b ++= ct.ids.keys.iterator.map(Type.Named(_))
      b.result()
    }

    // find co-occurrences FIXME: replace with Stack-based loop rather than
    // recursion?
    {
      def findCoOccs(ct: CT, pol: Boolean): Unit = {
        // lazy since we _may_ use it below
        lazy val occs = occurrences(ct)

        // repeatedly intersects type occurrences with each variable, leaving
        // coOccs with the set of types which always occur with each variable.
        ct.vars.foreach { v =>
          coOccs((v, pol)) = coOccs.get((v, pol)) match {
            case Some(os) => os.intersect(occs)
            case None     => occs
          }

          // if `v` is recursive, process its bound if we haven't done so already
          recs.get(v).foreach { ct0 =>
            if (!allVars.contains(v)) {
              allVars.add(v)
              findCoOccs(ct0, pol)
            }
          }

          allVars.add(v)
        }

        ct.overloads.foreach { case (_, v) => findCoOccs(v, pol) }

        ct.constructs.foreach { case (_, args) =>
          args.foreach(findCoOccs(_, pol))
        }

        ct.func.foreach { case (_, (ps, v, r)) =>
          ps.foreach(p => findCoOccs(p._2, !pol))
          v.map(v => findCoOccs(v._2, !pol))
          findCoOccs(r, pol)
        }

        ct.access.foreach { case (ps, v, r) =>
          ps.foreach(findCoOccs(_, !pol))
          v.map(findCoOccs(_, !pol))
          findCoOccs(r, pol)
        }

        ct.proj.foreach { case (p, r) =>
          findCoOccs(p, !pol)
          findCoOccs(r, pol)
        }

        ct.record.foreach { case (fs, w) =>
          fs.foreach { case (_, v) => findCoOccs(v, pol) }
          w.foreach(findCoOccs(_, pol))
        }

        ct.interface.foreach { fs =>
          fs.foreach { case (_, v) => findCoOccs(v, pol) }
        }

        ct.tuples.foreach { case (_, es) => es.foreach(findCoOccs(_, pol)) }

        ct.diffs.foreach { case (v, s) =>
          findCoOccs(v, pol)
          findCoOccs(s, pol)
        }
      }

      findCoOccs(ct, pol)
      aliases.values.foreach(findCoOccs(_, pol))
    }

    // Generate substitutions

    // A value of None means the variable is eliminated. A value of Some(varid)
    // means the variable is substituted with `varid`.
    val subs = MMap.empty[Type.Var, Option[Type.Var]]

    // Perform elimination of strictly positive or negative, non-recursive
    // variables.(#3 above)
    allVars.iterator.filterNot(recs.contains).foreach { v =>
      (coOccs.get((v, true)), coOccs.get((v, false))) match {
        case (Some(_), None) | (None, Some(_)) =>
          subs(v) = None
        case occ => assert(occ != (None, None))
      }
    }

    for {
      v <- allVars
      if !subs.contains(v)
      pol <- Seq(true, false)
    } {
      coOccs.getOrElse((v, pol), Set.empty).foreach {
        case w: Type.Var =>
          // Variable substitution (#1 above)
          if (
            // not the same var
            v != w &&
            // we have not already substituted w
            !subs.contains(w) &&
            // do not merge rec and non-rec vars
            (recs.contains(v) == recs.contains(w)) &&
            // v and w are mutually co-occurring
            coOccs((w, pol)).contains(v)
          ) {
            subs(w) = Some(v)

            recs.get(w) match {
              case Some(wCt) =>
                // remove w from the list of rec types, and rewrite v with v and
                // w's merged bounds.
                recs.remove(w)
                val vCt = recs(v)
                recs(v) = vCt.merge(wCt, pol)

              case None =>
                // merge the co-occurrences of v and w in the opposite polarity.
                val vCoOccs = coOccs((v, !pol))
                val wCoOccs = coOccs((w, !pol))
                // FIXME: this doesn't seem quite right where three variables
                // are involved, but follow's simple-sub's implementation. Explore
                // further?
                coOccs((v, !pol)) = vCoOccs.filter { t =>
                  t == v || wCoOccs.contains(t)
                }
            }
          }
        case ty @ (_: Type.Any | _: Type.Top | _: Type.Never | _: Type.Named |
            _: Type.Singleton) =>
          // Elimination based on concrete type equivalence (#2 above)
          if (coOccs.getOrElse((v, !pol), Set.empty).contains(ty)) {
            subs(v) = None
          }
        case _ => ()
      }
    }

    // Reconstruct the type after generating substitutions, above.
    def subst(ct: CT): CT =
      ct.copy(
        vars = ct.vars.flatMap(v => subs.get(v).getOrElse(Some(v))),
        overloads = ct.overloads.map { case (a, v) => a -> subst(v) },
        constructs = ct.constructs.map { case (n, args) =>
          (n, args.map(subst))
        },
        func = ct.func.map { case (i, (ps, v, ret)) =>
          (
            i,
            (
              ps.map(p => p._1 -> subst(p._2)),
              v.map(v => v._1 -> subst(v._2)),
              subst(ret)))
        },
        access = ct.access.map { case (ps, v, ret) =>
          (ps.map(subst), v.map(subst), subst(ret))
        },
        proj = ct.proj.map { case (p, r) => (subst(p), subst(r)) },
        record = ct.record.map { case (fs, w) =>
          (fs.map { case (l, fv) => (l, subst(fv)) }, w.map(subst))
        },
        interface = ct.interface.map { fs =>
          fs.map { case (l, fv) => (l, subst(fv)) }
        },
        tuples = ct.tuples.map { case (i, es) => (i, es.map(subst)) },
        diffs = ct.diffs.map { case (v, s) => (subst(v), subst(s)) }
      )

    CoalescedType(
      subst(ct),
      recs.map { case (v, ct) => (v, subst(ct)) },
      aliases.map { case (k, ct) => (k, subst(ct)) })
  }
}
