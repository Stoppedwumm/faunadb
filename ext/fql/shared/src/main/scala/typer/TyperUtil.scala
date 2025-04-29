package fql.typer

import fql.typer.Constraint.{ Use, Value }
import fql.typer.Type.{ Var, VarId }
import scala.collection.immutable.{ ArraySeq, ListSet }
import scala.collection.mutable.{ Map => MMap, Set => MSet }

trait TyperAlts { self: Typer =>

  protected def freeAlts(vctx: Typer.VarCtx)(implicit containerAlt: Alt) = {
    dbg("Free alts:")
    val alts = Set.newBuilder[Alt]
    vctx.foreach { case (_, sch) =>
      val (v, u) = typeAlts(sch.raw, false, containerAlt)
      dbg(s" - ${sch.raw}: ${(v ++ u).toSet}")
      alts ++= v
      alts ++= u
    }
    alts.result()
  }

  protected def typeArity(v: Constraint) = {
    val cache = MSet.empty[Type.Var]

    def go(v: Constraint, pol: Boolean): Int =
      v match {
        case Constraint.Lazy(v) => go(v, pol)

        case _: Type.Skolem => 0
        case v: Type.Var =>
          if (!cache.add(v)) {
            0
          } else {
            val bounds = if (pol) v.values else v.uses
            bounds.foldLeft(0) { case (ar, (b, _)) => ar max go(b, pol) }
          }
        // FIXME: Named needs to track arg polarity based on the defn.
        // Treat all as positive for now.
        case _: Type.Named => 0

        case _: Constraint.Unleveled => 0

        case v: Constraint.Rec =>
          v.fields.values.foldLeft(0)((ar, b) => ar max go(b, pol))
        case t: Type.Record =>
          val w = t.wildcard.foldLeft(0)((ar, b) => ar max go(b, pol))
          t.fields.values.foldLeft(w)((ar, b) => ar max go(b, pol))

        case u: Constraint.Interface => go(u.ret, pol)
        case t: Type.Interface       => go(t.ret, pol)

        case u: Constraint.Proj => 1 + go(u.ret, pol)

        case v: Constraint.Tup => v.elems.foldLeft(0)((ar, b) => ar max go(b, pol))
        case t: Type.Tuple     => t.elems.foldLeft(0)((ar, b) => ar max go(b, pol))

        case v: Constraint.Func  => v.params.size + go(v.ret, pol)
        case u: Constraint.Apply => u.args.size + go(u.ret, pol)
        case t: Type.Function    => t.params.size + go(t.ret, pol)

        case u: Constraint.Access => u.args.size + go(u.ret, pol)

        case v: Constraint.Intersect =>
          v.variants.map(_._2).foldLeft(0)((ar, b) => ar max go(b, pol))
        case u: Constraint.Union =>
          u.variants.map(_._2).foldLeft(0)((ar, b) => ar max go(b, pol))

        case t: Type.Intersect =>
          t.variants.foldLeft(0)((ar, b) => ar max go(b, pol))
        case t: Type.Union =>
          t.variants.foldLeft(0)((ar, b) => ar max go(b, pol))

        case v: Constraint.Diff =>
          go(v.value, pol)
        case u: Constraint.DiffGuard =>
          go(u.use, pol)
      }

    go(v, true)
  }

  // Returns the involved Alts for the given type, in value and use position.
  protected def typeAlts(
    v: Constraint,
    pol: Boolean,
    containerAlt: Alt): (ListSet[Alt], ListSet[Alt]) = {

    val valts = ListSet.newBuilder[Alt]
    val ualts = ListSet.newBuilder[Alt]
    val cache = MSet.empty[Type.Var]

    def go(v: Constraint, pol: Boolean): Unit =
      v match {
        case Constraint.Lazy(v) => go(v, pol)

        case _: Type.Skolem =>

        case v: Type.Var =>
          // first time we've encountered this variable
          if (cache.add(v)) {
            val (bounds, b) = if (pol) (v.values, valts) else (v.uses, ualts)
            bounds.foreach { case (c, a) =>
              b += a
              go(c, pol)
            }
          }

        // FIXME: Named needs to track arg polarity based on the defn.
        // Treat all as positive for now.
        case t: Type.Named => t.args.foreach(go(_, pol))

        case _: Constraint.Lit | _: Type.UnleveledType =>

        case v: Constraint.Rec => v.fields.values.foreach(go(_, pol))
        case t: Type.Record =>
          t.fields.values.foreach(go(_, pol))
          t.wildcard.foreach(go(_, pol))

        case u: Constraint.Interface => go(u.ret, pol)
        case t: Type.Interface       => go(t.ret, pol)

        case u: Constraint.Proj =>
          go(u.proj, !pol)
          go(u.ret, pol)

        case v: Constraint.Tup => v.elems.foreach(go(_, pol))
        case t: Type.Tuple     => t.elems.foreach(go(_, pol))

        case v: Constraint.Func =>
          v.params.foreach(p => go(p._2, !pol))
          v.variadic.foreach(p => go(p._2, !pol))
          go(v.ret, pol)
        case u: Constraint.Apply =>
          u.args.foreach(go(_, !pol))
          go(u.ret, pol)
        case t: Type.Function =>
          t.params.foreach(p => go(p._2, !pol))
          t.variadic.foreach(p => go(p._2, !pol))
          go(t.ret, pol)

        case u: Constraint.Access =>
          u.args.foreach(go(_, !pol))
          go(u.ret, pol)

        case v: Constraint.Intersect =>
          v.variants.foreach { case (a, v) =>
            valts += a
            go(v, pol)
          }
        case u: Constraint.Union =>
          u.variants.foreach { case (a, u) =>
            ualts += a
            go(u, pol)
          }

        case i: Type.Intersect => i.variants.foreach(go(_, pol))
        case u: Type.Union     => u.variants.foreach(go(_, pol))

        case v: Constraint.Diff =>
          go(v.value, pol)
          go(v.sub, pol)
        case u: Constraint.DiffGuard =>
          go(u.sub, pol)
          go(u.use, pol)
      }

    go(v, pol)

    (
      valts.result().filter(_.alevel > containerAlt.alevel),
      ualts.result().filter(_.alevel > containerAlt.alevel))
  }

  protected def findProjections(v: Constraint): ListSet[Constraint.Proj] = {
    val projs = ListSet.newBuilder[Constraint.Proj]
    val cache = MSet.empty[Type.Var]

    def go(v: Constraint, pol: Boolean): Unit =
      v match {
        case Constraint.Lazy(v) => go(v, pol)

        case _: Type.Skolem =>

        case v: Type.Var =>
          // first time we've encountered this variable
          if (cache.add(v)) {
            val bounds = if (pol) v.values else v.uses
            bounds.foreach { case (c, _) =>
              go(c, pol)
            }
          }

        // FIXME: Named needs to track arg polarity based on the defn.
        // Treat all as positive for now.
        case t: Type.Named => t.args.foreach(go(_, pol))

        case _: Constraint.Lit | _: Type.UnleveledType =>

        case v: Constraint.Rec => v.fields.values.foreach(go(_, pol))
        case t: Type.Record =>
          t.fields.values.foreach(go(_, pol))
          t.wildcard.foreach(go(_, pol))

        case u: Constraint.Interface => go(u.ret, pol)
        case t: Type.Interface       => go(t.ret, pol)

        case u: Constraint.Proj =>
          projs += u
          go(u.proj, !pol)
          go(u.ret, pol)

        case v: Constraint.Tup => v.elems.foreach(go(_, pol))
        case t: Type.Tuple     => t.elems.foreach(go(_, pol))

        case v: Constraint.Func =>
          v.params.foreach(p => go(p._2, !pol))
          v.variadic.foreach(p => go(p._2, !pol))
          go(v.ret, pol)
        case u: Constraint.Apply =>
          u.args.foreach(go(_, !pol))
          go(u.ret, pol)
        case t: Type.Function =>
          t.params.foreach(p => go(p._2, !pol))
          t.variadic.foreach(p => go(p._2, !pol))
          go(t.ret, pol)

        case u: Constraint.Access =>
          u.args.foreach(go(_, !pol))
          go(u.ret, pol)

        case v: Constraint.Intersect =>
          v.variants.foreach { case (_, v) =>
            go(v, pol)
          }
        case u: Constraint.Union =>
          u.variants.foreach { case (_, u) =>
            go(u, pol)
          }

        case i: Type.Intersect => i.variants.foreach(go(_, pol))
        case u: Type.Union     => u.variants.foreach(go(_, pol))

        case v: Constraint.Diff =>
          go(v.value, pol)
          go(v.sub, pol)
        case u: Constraint.DiffGuard =>
          go(u.use, pol)
          go(u.sub, pol)
      }

    go(v, pol = true)

    projs.result()
  }

  /** Generate an Intersect constraint based on Alt visibility and dependencies.
    */
  def annealValue(
    ty: Constraint.Value,
    containerAlt: Alt,
    level: Type.Level,
    free: Set[Alt] = Set.empty): Constraint.Value =
    annealConstraint(ty, containerAlt, true, level, free) { (ty, filter) =>
      freshenV(level.decr, ty, Some(filter))(
        containerAlt,
        level,
        MMap.empty,
        MMap.empty
      )
    } { Constraint.Intersect(_) }

  /** Generate an Union constraint based on Alt visibility and dependencies.
    */
  protected def annealUse(
    ty: Constraint.Use,
    containerAlt: Alt,
    level: Type.Level,
    free: Set[Alt] = Set.empty): Constraint.Use =
    annealConstraint(ty, containerAlt, false, level, free) { (ty, filter) =>
      freshenU(level.decr, ty, Some(filter))(
        containerAlt,
        level,
        MMap.empty,
        MMap.empty
      )
    } { Constraint.Union(_) }

  private def annealConstraint[T <: Constraint](
    ty: T,
    containerAlt: Alt,
    pol: Boolean,
    level: Type.Level,
    free: Set[Alt])(freshen: (T, Map[(Alt, Boolean), Alt]) => T)(
    overload: ArraySeq[(Alt, T)] => T): T = {

    // internal method so return works with dbgFrame
    def anneal0(): T = {
      val (valts, ualts) = typeAlts(ty, pol, containerAlt)
      val (altOuts, altDeps0) = if (pol) (valts, ualts) else (ualts, valts)
      val altDeps = altDeps0.filterNot(_ == containerAlt)

      def isActive(a: Alt) =
        a == containerAlt || altDeps.contains(a) || free.contains(a)
      def hasConflict(a: Alt) = {
        val (v, u) = if (pol) (containerAlt, a) else (a, containerAlt)
        Alt.hasConflict(v, u)
      }

      def dbgStatus(a: Alt) = {
        var str = ""
        if (isActive(a)) str += "+"
        if (a == containerAlt) str += "B"
        if (altDeps.contains(a)) str += "D"
        if (free.contains(a)) str += "F"
        if (hasConflict(a)) str = s"!$str!"
        if (str.nonEmpty) str = s"($str)"
        str
      }

      // FIXME: this debug output is a bit out of date
      if (dbgEnabled) {
        def lstr(i: Iterable[Any]) = if (i.isEmpty) "-" else i.mkString(", ")
        dbg(s" ~ Base:   $containerAlt")
        if (free.nonEmpty) dbg(s" + Free:  ${lstr(free)}")
        dbg(s" ~ Deps:   ${if (pol) "Uses" else "Values"} ${lstr(altDeps)}")
      }

      // FIXME: not sure of exact logic here
      if (altOuts.isEmpty || altOuts.forall(_ == containerAlt)) {
        dbg(s"anneal skipped: $ty (${altOuts.mkString(", ")})")
        return ty
      }

      val paths = altOuts.view.map(_.outerGrp.valuePaths).reduceLeft(_ | _)
      val (depSets, baseSet) = paths.unifiedSets
      val freeDeps = depSets.view
        .flatMap { case (d, outs) =>
          if (free(d)) {
            outs.toSet.view.flatMap(_.map(_ -> d))
          } else {
            Nil
          }
        }
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).toSet)
        .toMap

      val validSets =
        if (altDeps.isEmpty && baseSet.nonEmpty) {
          // altDeps is empty, meaning no overloaded input, but there's containerAlt
          // input which anchors the result.
          ListSet(baseSet -> ListSet(containerAlt))
        } else {
          // if altDeps is empty, then the result is a partial, inner type, or
          // there is a circular overload involved (i.e. an overloaded function
          // passed to an overloaded HOF). In that case we conservatively
          // activate all paths.
          val active = if (altDeps.nonEmpty) altDeps else depSets.keySet

          val combos = active.groupBy(_.outerGrp).foldLeft(Set(ListSet.empty[Alt])) {
            case (acc, (_, grp)) => acc.flatMap(s => grp.map(s + _))
          }

          val sets = combos.flatMap { dc =>
            val union =
              dc
                .foldLeft(Alt.OrSet.MinimalSet()) { (m, d) =>
                  m += depSets.getOrElse(d, Alt.OrSet(Set(baseSet)))
                }
                .minimal

            val hasDisallowedUnion =
              union.groupBy(_.outerGrp).exists { case (g, ss) =>
                g.isExclusive && ss.sizeIs > 1
              }

            if (!hasDisallowedUnion) Some(union -> dc) else None
          }

          // if altDeps is empty, we need to unify the the resulting set to
          // eliminate redundant alternatives
          // TODO: move this algo out into a helper in the right place.
          if (altDeps.isEmpty) {
            var ret = ListSet.empty[ListSet[Alt]]
            val altsSet = sets.map(_._1)

            altsSet.foreach { sc =>
              var sub = false
              altsSet.foreach { v =>
                if ((sc != v) && ((sc | v) == sc)) sub = true
              }
              if (!sub) ret += sc
            }
            ret.map(s => s -> s)
          } else {
            sets
          }
        }

      if (dbgEnabled) {
        dbg(" ~ Sets:")
        validSets.foreach { case (pset, nset) =>
          val pstr = pset.map(a => s"$a${dbgStatus(a)}").mkString(", ")
          val nstr = nset.map(a => s"$a${dbgStatus(a)}").mkString(", ")
          dbg(s"   - $pstr -> $nstr")
        }
      }

      if (validSets.sizeIs == 1) {
        dbgFrame("Single variant:") {
          val (pset0, nset0) = validSets.head
          val pset = pset0.flatMap(_.lineage)
          val nset = nset0.flatMap(_.lineage)
          val pfilter = pset.view.map((_, pol) -> containerAlt)
          val nfilter = nset.view.map((_, !pol) -> containerAlt)
          freshen(ty, pfilter.concat(nfilter).toMap)
        }
      } else {
        dbgFrame("Multi-variant:") {
          val exclusive = typeArity(ty) > 1
          val ngrp = freshAltGrp(validSets.size, exclusive, containerAlt, level)

          val variants = validSets.view
            .zip(ngrp.childAlts)
            .map { case ((pset0, nset0), na) =>
              val pset = pset0.flatMap(_.lineage)
              val nset = nset0.flatMap(_.lineage)

              val pfilter = pset.view.map { a =>
                freeDeps.getOrElse(a, Set.empty).foreach { f =>
                  dbg(s"link free $f -> $na")
                  if (pol) Alt.link(f, na) else Alt.link(na, f)
                }
                (a, pol) -> na
              }
              val nfilter = nset.view.map((_, !pol) -> na)
              na -> freshen(ty, (pfilter.concat(nfilter)).toMap)
            }
            .to(ArraySeq)

          overload(variants)
        }
      }
    }

    dbgFrame(s"Anneal $ty") {
      anneal0()
    }
  }
}

/** var and alt "freshening", used in instantiation.
  * FIXME: expand on description
  */
trait TyperFreshen { self: Typer =>

  protected def freshenV(
    lim: Type.Level,
    v: Value,
    altFilter: Option[Map[(Alt, Boolean), Alt]] = None)(
    implicit containerAlt: Alt,
    lvl: Type.Level,
    acache: MMap[Alt, Alt],
    vcache: MMap[VarId, Var]): Value =
    v match {
      case Constraint.Lazy(v) => freshenV(lim, v, altFilter)

      case ty: Type => freshenT(lim, ty, altFilter)

      case _ if v.level <= lim => v

      case c: Constraint.Lit => c
      case r: Constraint.Rec =>
        r.copy(fields = r.fields.map { case (l, t) =>
          (l, freshenV(lim, t, altFilter))
        })
      case t: Constraint.Tup =>
        t.copy(elems = t.elems.map(freshenV(lim, _, altFilter)))
      case f: Constraint.Func =>
        f.copy(
          params = f.params.map(p => p._1 -> freshenU(lim, p._2, altFilter)),
          variadic = f.variadic.map(v => v._1 -> freshenU(lim, v._2, altFilter)),
          ret = freshenV(lim, f.ret, altFilter)
        )
      case i: Constraint.Intersect =>
        i.copy(variants = i.variants.flatMap { case (a, v) =>
          freshenA(lim, a, true, altFilter).map((_, freshenV(lim, v, altFilter)))
        })
      case d: Constraint.Diff =>
        d.copy(
          value = freshenV(lim, d.value, altFilter),
          sub = freshenT(lim, d.sub, altFilter))
    }

  protected def freshenU(
    lim: Type.Level,
    u: Use,
    altFilter: Option[Map[(Alt, Boolean), Alt]])(
    implicit containerAlt: Alt,
    lvl: Type.Level,
    acache: MMap[Alt, Alt],
    vcache: MMap[VarId, Var]): Use =
    u match {
      case ty: Type => freshenT(lim, ty, altFilter)

      case _ if u.level <= lim => u

      case s: Constraint.Interface =>
        s.copy(ret = freshenU(lim, s.ret, altFilter))
      case s: Constraint.Proj =>
        s.copy(
          proj = freshenV(lim, s.proj, altFilter),
          ret = freshenU(lim, s.ret, altFilter))
      case a: Constraint.Apply =>
        a.copy(
          args = a.args.map(freshenV(lim, _, altFilter)),
          ret = freshenU(lim, a.ret, altFilter))
      case a: Constraint.Access =>
        a.copy(
          args = a.args.map(freshenV(lim, _, altFilter)),
          ret = freshenU(lim, a.ret, altFilter))
      case u: Constraint.Union =>
        u.copy(variants = u.variants.flatMap { case (a, u) =>
          freshenA(lim, a, false, altFilter).map((_, freshenU(lim, u, altFilter)))
        })
      case d: Constraint.DiffGuard =>
        d.copy(
          sub = freshenT(lim, d.sub, altFilter),
          use = freshenU(lim, d.use, altFilter))
    }

  private def freshenT(
    lim: Type.Level,
    ty: Type,
    altFilter: Option[Map[(Alt, Boolean), Alt]])(
    implicit containerAlt: Alt,
    lvl: Type.Level,
    acache: MMap[Alt, Alt],
    vcache: MMap[VarId, Var]): Type =
    ty match {
      case _ if ty.level <= lim =>
        dbg(s"freshen skipped $ty")
        ty

      // TODO: this case is a sanity check against skolems leaking out from
      // let signature checks. needs more test cases to ensure it cannot happen.
      case t: Type.Skolem => sys.error(s"Unexpected mis-leveled $t in freshen")

      case v: Var =>
        vcache.getOrElse(
          v.id, {
            val nv = freshVar(v.span)
            dbg(s"freshen $v -> $nv")
            vcache += (v.id -> nv)
            nv.values = v.values.flatMap { case (v, a) =>
              freshenA(lim, a, true, altFilter).map { a0 =>
                (freshenV(lim, v, altFilter), a0)
              }
            }
            nv.uses = v.uses.flatMap { case (u, a) =>
              freshenA(lim, a, false, altFilter).map { a0 =>
                (freshenU(lim, u, altFilter), a0)
              }
            }
            if (v.poisoned) nv.poison()
            nv
          }
        )

      case t @ (_: Type.Any | _: Type.Top | _: Type.Never | _: Type.Singleton) => t
      case c: Type.Named => c.copy(args = c.args.map(freshenT(lim, _, altFilter)))
      case f: Type.Function =>
        f.copy(
          params = f.params.map(p => p._1 -> freshenT(lim, p._2, altFilter)),
          variadic = f.variadic.map(v => v._1 -> freshenT(lim, v._2, altFilter)),
          ret = freshenT(lim, f.ret, altFilter)
        )
      case u: Type.Union =>
        u.copy(variants = u.variants.map(freshenT(lim, _, altFilter)))
      case i: Type.Intersect =>
        i.copy(variants = i.variants.map(freshenT(lim, _, altFilter)))

      case r: Type.Record =>
        r.copy(
          fields = r.fields.map { case (l, t) => (l, freshenT(lim, t, altFilter)) },
          wildcard = r.wildcard.map(freshenT(lim, _, altFilter)))
      case i: Type.Interface =>
        i.copy(ret = freshenT(lim, i.ret, altFilter))

      case t: Type.Tuple =>
        t.copy(elems = t.elems.map(freshenT(lim, _, altFilter)))
    }

  private def freshenA(
    lim: Type.Level,
    alt: Alt,
    pol: Boolean,
    altFilter: Option[Map[(Alt, Boolean), Alt]])(
    implicit lvl: Type.Level,
    containerAlt: Alt,
    acache: MMap[Alt, Alt]): Option[Alt] = {

    def freshen0(alt: Alt): Alt =
      if (alt.vlevel <= lim) {
        alt
      } else {
        acache.get(alt) match {
          case Some(a) => a
          case None    =>
            // FIXME: I don't think we need the container alt here at all
            val nparent =
              Option
                .when(alt.parent.isOuterGrp)(containerAlt)
                .orElse(alt.parent.parent.map(freshen0))
            val ngrp =
              freshAltGrp(
                alt.parent.size,
                alt.parent.isExclusive,
                nparent,
                alt.alevel,
                lvl)
            dbg(s"freshen alt ${alt.parent} -> $ngrp")
            val zipped = alt.parent.childAlts.view.zip(ngrp.childAlts)
            zipped.foreach { case (a, na) =>
              acache(a) = na
            }
            zipped.foreach { case (a, na) =>
              dbg(s" + alt $a -> $na")
              a.values.foreach { va =>
                dbgFrame(s" + value dep $va") {
                  na.addValue(freshen0(va))
                }
              }
              a.uses.foreach { ua =>
                dbgFrame(s" + use dep $ua") {
                  na.addUse(freshen0(ua))
                }
              }
            }
            acache(alt)
        }
      }

    altFilter match {
      case None => Some(freshen0(alt))
      case Some(filter) =>
        filter.get((alt, pol)) match {
          case None if alt.vlevel <= lim =>
            val a0 = freshen0(alt)
            dbg(s"freshened $alt => $a0")
            Some(a0)
          case None =>
            dbg(s"filtered $alt -> None")
            None
          case Some(fa) =>
            dbg(s"filtered $alt -> Some($fa)")
            Some(fa)
        }
    }
  }
}

/** extrusion
  * FIXME: expand on description
  */
trait TyperExtrude { self: Typer =>

  protected def extrudeV(v: Value)(
    implicit lvl: Type.Level,
    cache: MMap[(VarId, Boolean), Var] = MMap.empty): Value =
    v match {
      case Constraint.Lazy(v) => extrudeV(v)

      case ty: Type => extrudeT(ty, true)

      case _ if v.level <= lvl => v

      case c: Constraint.Lit => c
      case r: Constraint.Rec =>
        r.copy(fields = r.fields.map { case (l, t) => (l, extrudeV(t)) })
      case t: Constraint.Tup =>
        t.copy(elems = t.elems.map(extrudeV))
      case f: Constraint.Func =>
        f.copy(
          params = f.params.map(p => p._1 -> extrudeU(p._2)),
          variadic = f.variadic.map(v => v._1 -> extrudeU(v._2)),
          ret = extrudeV(f.ret))
      case i: Constraint.Intersect =>
        i.copy(variants = i.variants.map(t => t.copy(_2 = extrudeV(t._2))))
      case d: Constraint.Diff =>
        d.copy(value = extrudeV(d.value), sub = extrudeT(d.sub, true))
    }

  protected def extrudeU(u: Use)(
    implicit lvl: Type.Level,
    cache: MMap[(VarId, Boolean), Var] = MMap.empty): Use =
    u match {
      case ty: Type => extrudeT(ty, false)

      case _ if u.level <= lvl => u

      case s: Constraint.Interface =>
        s.copy(ret = extrudeU(s.ret))
      case s: Constraint.Proj =>
        s.copy(proj = extrudeV(s.proj), ret = extrudeU(s.ret))
      case a: Constraint.Apply =>
        a.copy(args = a.args.map(extrudeV), ret = extrudeU(a.ret))
      case a: Constraint.Access =>
        a.copy(args = a.args.map(extrudeV), ret = extrudeU(a.ret))
      case u: Constraint.Union =>
        u.copy(variants = u.variants.map(t => t.copy(_2 = extrudeU(t._2))))
      case d: Constraint.DiffGuard =>
        d.copy(sub = extrudeT(d.sub, true), use = extrudeU(d.use))
    }

  private def extrudeT(ty: Type, pol: Boolean)(
    implicit lvl: Type.Level,
    cache: MMap[(VarId, Boolean), Var]): Type =
    ty match {
      // TODO: this case is a sanity check against skolems leaking out from
      // let signature checks. needs more test cases to ensure it cannot happen.
      case t: Type.Skolem => sys.error(s"Unexpected skolem `${t.name}` in extrude")

      case _ if ty.level <= lvl => ty

      case v: Var =>
        cache.getOrElse(
          (v.id, pol), {
            val nv = freshVar(v.span)
            dbg(s"extrude $v -> $nv")
            cache += ((v.id, pol) -> nv)
            if (pol) {
              v.addUse(nv, rootAlt)
              nv.values = v.values.map { case (v, a) => (extrudeV(v), a) }
            } else {
              v.addValue(nv, rootAlt)
              nv.uses = v.uses.map { case (u, a) => (extrudeU(u), a) }
            }
            if (v.poisoned) nv.poison()
            nv
          }
        )

      case t @ (_: Type.Any | _: Type.Top | _: Type.Never | _: Type.Singleton) => t
      case c: Type.Named => c.copy(args = c.args.map(extrudeT(_, pol)))
      case f: Type.Function =>
        f.copy(
          params = f.params.map(p => p._1 -> extrudeT(p._2, !pol)),
          variadic = f.variadic.map(v => v._1 -> extrudeT(v._2, !pol)),
          ret = extrudeT(f.ret, pol))
      case u: Type.Union     => u.copy(variants = u.variants.map(extrudeT(_, pol)))
      case i: Type.Intersect => i.copy(variants = i.variants.map(extrudeT(_, pol)))

      case r: Type.Record =>
        r.copy(
          fields = r.fields.map { case (l, t) => (l, extrudeT(t, pol)) },
          wildcard = r.wildcard.map(extrudeT(_, pol)))

      case i: Type.Interface => i.copy(ret = extrudeT(i.ret, pol))

      case t: Type.Tuple => t.copy(elems = t.elems.map(extrudeT(_, pol)))
    }

  protected def extrudeA(a: Alt)(implicit lvl: Type.Level): Alt =
    if (a.vlevel <= lvl) {
      a
    } else {
      val egrp = a.outerGrp.extrudes.getOrElse(
        (lvl -> a.parent), {
          val nparent = a.parent.parent.map(extrudeA(_))
          val ngrp =
            freshAltGrp(a.parent.size, a.parent.isExclusive, nparent, a.alevel, lvl)

          dbg(s"extrude alt grp ($lvl, ${a.parent}) -> $ngrp")

          a.outerGrp.update(s =>
            s.copy(extrudes = s.extrudes + ((lvl -> a.parent) -> ngrp)))

          a.parent.childAlts.view.zip(ngrp.childAlts).foreach { case (a, na) =>
            Alt.link(a, na)
            Alt.link(na, a)
          }
          ngrp
        }
      )
      egrp.childAlts(a.idx)
    }

  // This is a very, very simplified version of constrain0, which
  // just checks for nulls. Type.Var doesn't need to be handled, as
  // we will never infer an object (we only infer interfaces).
  def allowMissingField(use: Constraint.Use): Boolean = use match {
    case Type.Null         => true
    case Type.Union(us, _) => us.exists(allowMissingField(_))
    case _                 => false
  }
}
