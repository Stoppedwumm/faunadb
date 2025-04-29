package fql.typer

import fql.error.TypeError
import scala.collection.immutable.{ ArraySeq => ASeq, ListSet => LSet }
import scala.collection.mutable.{
  ArrayBuffer => ABuffer,
  LinkedHashSet => MSet,
  ListBuffer => LBuffer,
  Map => MMap
}
import Constraint.{ Use, Value }
import TyperDebug.{ dbg, dbgEnabled, dbgFrame }

/** TypeConstraintCheck represents the result of checking the validity of a value/use contraint. */
object TypeConstraintCheck {
  private type TC = TypeConstraintCheck
  private type TCF = TypeConstraintFailure

  // we rely on object equality for determining set membership elsewhere in this
  // file.
  class Ref {
    private var chk: TC = _

    private var failed = false
    private var primaryFails = LSet.empty[Fail]
    private[TypeConstraintCheck] val vars = MSet.empty[Var]
    private[TypeConstraintCheck] val prunedAlts = MSet.empty[Alt]
    private[TypeConstraintCheck] val secondaryFails = MSet.empty[Fail]

    private def resetState() = {
      failed = false
      primaryFails = LSet.empty
      vars.clear()
      prunedAlts.clear()
      secondaryFails.clear()
    }

    def isFailed = failed

    def errors: (List[TCF], List[TypeError]) = {
      if (!isFailed) {
        return (Nil, Nil)
      }

      val cfs = LBuffer.empty[TCF]
      val tes = LBuffer.empty[TypeError]

      primaryFails.foreach { f =>
        tes ++= f.tes
        cfs ++= f.cfs
      }

      if (cfs.isEmpty && tes.isEmpty) {
        secondaryFails.foreach { f =>
          tes ++= f.tes
          if (f.valt == Alt.Base || prunedAlts.contains(f.valt)) {
            cfs ++= f.cfs
          }
        }
      }

      (cfs.toList, tes.toList)
    }

    def get = {
      require(chk ne null)
      chk
    }

    def set(c: TC) = {
      require(chk eq null)
      chk = c
    }

    private def prune(): Unit = {
      resetState()
      primaryFails = chk.prune(this)
      failed = primaryFails.nonEmpty
      chk = chk.simplified
    }

    private def rechecks =
      prunedAlts.view.flatMap(_.outerGrp._state.chkdeps)

    def runCheck()(implicit dbgCtx: TyperDebugCtx): Unit =
      dbgFrame("CHECK") {
        // steps:
        // 1. mark check graph:
        //   - register check as dep on alts
        // 2. prune main check
        //   - collect fatal, non-fatal errors.
        //   - collect alt dep checks to recheck.
        //   - simplify, eliminate non-fatal errors.
        // 3. re-eval and prune rechecks.
        //   - any dep check failure indicates non-fatal errors were fatal
        // Constraints:
        //   - at least one expr alt input per group must remain valid.
        //   - an exclusive alt cannot have two required alts.

        val before = Option.when(dbgEnabled)(chk.display)
        val beforeFlat = Option.when(dbgEnabled)(chk.displayFlat)

        if (dbgEnabled) {
          dbg(before.get)
          if (beforeFlat != before) {
            dbg("--- flattened ---")
            dbg(beforeFlat.get)
          }
        }

        // mark check graph
        chk.mark(this)(MSet.empty)

        // prune & simplify main check
        prune()

        val pruned = Option.when(dbgEnabled)(chk.display)
        if (dbgEnabled && pruned != before) {
          dbg("--- pruned ---")
          dbg(pruned.get)
        }

        // recheck deps
        var deps = LSet(this)

        // for the initial setup, we want to exclude the main check, since it
        // was just pruned above.
        deps ++= rechecks
        deps -= this

        if (!failed && deps.nonEmpty) {
          dbg("\nRepruning Deps")
        }

        while (!failed && deps.nonEmpty) {
          val iter = deps.iterator
          deps = LSet.empty

          while (!failed && iter.hasNext) {
            val d = iter.next()

            dbg("--- before ---")
            dbg(d.get.display)

            d.prune()

            dbg(s"--- after${if (d.isFailed) " (FAILED)" else ""} ---")
            dbg(d.get.display)

            failed ||= d.isFailed
            if (!failed) deps ++= d.rechecks
          }
        }

        if (dbgEnabled && isFailed) {
          dbg("--- FAILURES ---")
          val (ces, tes) = errors
          ces.foreach(f => dbg(f))
          tes.foreach(f => dbg(f))
        }
      }

    override def toString() = s"Ref($chk)"
  }

  // variants

  def all(chks: Iterable[TC]) =
    chks.reduceLeftOption(_ and _).getOrElse(Empty)

  def combos(chks: Iterable[TC]) =
    chks.reduceLeftOption(_ combine _).getOrElse(Empty)

  case object Empty extends TypeConstraintCheck {
    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) = ()

    def prune(cref: Ref) = LSet.empty
  }

  sealed trait Concrete extends TypeConstraintCheck {
    val value: Value
    val valt: Alt
    val use: Use
    val ualt: Alt

    // Do not consider the check pruned if valt == ualt, as Alt-internal checks
    // should always be propagated as-is.
    def isPruned = (valt != ualt) && (valt.isPruned2 || ualt.isPruned2)

    // When re-pruning a check, we return this instead of Ok or Sub when one of
    // this check's alts is pruned. This reuses the existing propagation path to
    // fail if all Alts in a group have been pruned.
    def prunedErr = Fail(value, valt, use, ualt, ASeq.empty, ASeq.empty)
  }

  final case class Ok(value: Value, valt: Alt, use: Use, ualt: Alt)
      extends Concrete {

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) =
      markAlts(valt, ualt, cref)

    def prune(cref: Ref) = if (isPruned) LSet(prunedErr) else LSet.empty
  }

  final case class Fail(
    value: Value,
    valt: Alt,
    use: Use,
    ualt: Alt,
    tes: ASeq[TypeError],
    cfs: ASeq[TCF])
      extends Concrete {

    def isFatal = tes.nonEmpty

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) =
      markAlts(valt, ualt, cref)

    def prune(cref: Ref) = {
      if (!isPruned) {
        cref.secondaryFails += this
      }
      LSet(this)
    }
  }

  final case class Sub(value: Value, valt: Alt, use: Use, ualt: Alt, chk: TC)
      extends Concrete {

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) = {
      markAlts(valt, ualt, cref)
      chk.mark(cref)
    }

    def prune(cref: Ref) = chk.prune(cref)
  }

  final case class VAlias(value: Value, valt: Alt, chk: TC)
      extends TypeConstraintCheck {

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) = {
      // It should not be necessary to call markAlts(), as all value/use Alts in chk
      // should be in the same Grp as `valt`/`ualt`.
      chk.mark(cref)
    }

    // If prune gets directly called on an Alias, this means it wasn't a child
    // of a Combos node, so lift it into one.
    def prune(cref: Ref) = Combos(LSet(chk)).prune(cref)
  }

  final case class Var(tv: Type.Var) extends TypeConstraintCheck {
    def chk: TC = tv._state.chk

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) =
      if (cache.add(tv)) {
        chk.mark(cref)
      }

    def prune(cref: Ref) =
      if (cref.vars.add(this)) chk.prune(cref) else LSet.empty
  }

  sealed trait AllOrCombos extends TypeConstraintCheck {
    val chks: LSet[TC]

    def mark(cref: Ref)(implicit cache: MSet[Type.Var]) =
      chks.foreach(_.mark(cref))
  }

  final case class All(chks: LSet[TC]) extends AllOrCombos {
    def prune(cref: Ref) = chks.flatMap(_.prune(cref))
  }

  final case class Combos(chks: LSet[TC]) extends AllOrCombos {
    def flattened: LSet[(ASeq[(Value, Alt)], TC)] = {
      val b = LSet.newBuilder[(ASeq[(Value, Alt)], TC)]
      val cache = MSet.empty[Var]

      def go(c: TC)(implicit path: ASeq[(Value, Alt)]): Unit =
        c match {
          case v: Var     => if (cache.add(v)) { go(v.chk) }
          case Combos(cs) => cs.foreach(go)

          // need to prepend the alias path
          case VAlias(v, a, c) => go(c)(path :+ (v -> a))

          case c => b += path -> c
        }

      go(this)(ASeq.empty)
      b.result()
    }

    def prune(cref: Ref) = {
      //                 value  valt        ugrp          ualt
      val concretes =
        MMap.empty[ASeq[(Value, Alt)], MMap[Alt.Grp, MMap[Alt, ABuffer[Concrete]]]]
      var others = LSet.empty[TC]

      flattened.foreach {
        // FIXME: narrow type of flattened to make this not a runtime error
        case (_, _: Var | _: Combos | _: VAlias) => sys.error("unreachable")

        case (_, Empty) =>

        // `All` subchecks act as parens, so resolve independently.
        case (_, a: All) => others += a

        // Fatal Fails always are emitted.
        case (_, f: Fail) if f.isFatal => others += f

        // Concretes with the same ualt and valt do not participate in alt
        // resolution logic.
        case (_, c: Concrete) if c.valt == c.ualt => others += c

        case (vpath, c: Concrete) =>
          concretes
            .getOrElseUpdate(vpath :+ (c.value -> c.valt), MMap.empty)
            .getOrElseUpdate(c.ualt.outerGrp, MMap.empty)
            .getOrElseUpdate(c.ualt, ABuffer.empty)
            .append(c)
      }

      val filteredfails = ABuffer.empty[Fail]
      val failedvalts = MMap.empty[Alt.Grp, MMap[Alt, ABuffer[Fail]]]

      concretes.keys.foreach { vpath =>
        val (_, valt) = vpath.last

        // pad out checks with others under a prefix of our path
        val uses = MMap.empty[Alt.Grp, MMap[Alt, ABuffer[Concrete]]]
        vpath.inits.foreach { path =>
          concretes.getOrElse(path, Nil).foreach { case (ugrp, ualts) =>
            ualts.foreach { case (ualt, chks) =>
              uses
                .getOrElseUpdate(ugrp, MMap.empty)
                .getOrElseUpdate(ualt, ABuffer.empty) ++= chks
            }
          }
        }

        uses.foreach { case (ugrp, ualts) =>
          val oks = ABuffer.empty[Concrete]
          val fails = ABuffer.empty[Fail]

          ualts.foreach { case (ua @ _, tcs) =>
            var allOk = true

            tcs.foreach { c =>
              val fs = c.prune(cref)
              if (fs.nonEmpty) allOk = false
              fails ++= fs
            }

            // at least one use in the alt failed
            if (allOk) oks ++= tcs
          }

          // Need to recheck deps if:
          // - the valt fails here and must be pruned pruned
          // - the valt partially fails. Recheck chk deps which contain an
          //   exclusive alt. FIXME: right now we just recheck everything.

          // check for invalid conjoinedness
          if (oks.nonEmpty && ugrp != Alt.Grp.Base) {

            // we're updating grp state here before we know if it's valid,
            // meaning we need to undo it if the conjoinedness check fails.
            // Otherwise subsequent checks can incorrectly fail.
            val prevstate = ugrp._state

            ugrp.update { s =>
              val deps = oks.map(_.ualt).toSet
              val depMap = s.depMap.get(valt).fold(Set(deps))(_ + deps)
              s.copy(depMap = s.depMap + (valt -> depMap))
            }

            var invalidConjoined = false
            val iter = ugrp.transitiveUseGrps.iterator

            while (!invalidConjoined && iter.hasNext) {
              val grp = iter.next()

              if (grp.isExclusive) {

                // roll up deps to the first exclusive ancestor
                def ex(a: Alt): Alt =
                  if (a.parent.isExclusive) a else ex(a.parent.parent.get)

                val depMap =
                  grp.transitiveDepMap.view.mapValues(_.map(_.map(ex))).toMap

                // FIXME: explain what exactly this code is doing. At a
                // high level its determining if across all value Alt Grps, any
                // two of the use Grps Alts are exclusively required.
                if (depMap.nonEmpty) {
                  val depSets = depMap
                    .map { case (a, deps) => a -> deps.reduceLeft(_ & _) }
                    .groupBy(_._1.outerGrp)
                    .values
                    .map(_.map(_._2).toSet)
                    .foldLeft(Set(Set.empty[Set[Alt]])) { (sss, ss1) =>
                      sss.flatMap { ss => ss1.map(ss + _) }
                    }

                  val isConjoined = depSets.forall(_.reduceLeft(_ & _).isEmpty)
                  invalidConjoined ||= isConjoined
                }
              }
            }

            if (invalidConjoined) {
              // we weren't really OK.
              oks.clear()
              // put back state if we failed
              ugrp.update(_ => prevstate)
            }
          }

          // This valt is dead
          if (oks.isEmpty) {
            failedvalts
              .getOrElseUpdate(valt.outerGrp, MMap.empty)
              .update(valt, fails)

            if (valt == Alt.Base) {
              // base valt: fatal failure
              filteredfails ++= fails
            } else {
              // alt must be pruned.
              if (!valt.isPruned2) {
                valt.markPruned2()
                cref.prunedAlts += valt
              }
            }
          }
        }
      }

      failedvalts.foreach { case (vgrp, valts) =>
        val livefailed = valts.keys.filterNot(_.isPruned2)
        if (livefailed.size == vgrp.liveAlts2.size) {
          // all vgrp alts failed: fatal failure
          valts.values.foreach(filteredfails ++= _)
        }
      }

      others.flatMap(_.prune(cref)) ++ filteredfails
    }
  }
}

sealed trait TypeConstraintCheck {
  import TypeConstraintCheck._

  def mark(cref: Ref)(implicit cache: MSet[Type.Var]): Unit

  def prune(cref: Ref): LSet[Fail]

  // FIXME: do we need to attach checks to lower level alts?
  protected def markAlts(va: Alt, ua: Alt, cref: Ref) =
    markAltGrps(va.outerGrp, ua.outerGrp, cref)

  protected def markAltGrps(vgrp: Alt.Grp, ugrp: Alt.Grp, cref: Ref) =
    if (vgrp != ugrp) {
      if (vgrp.alevel >= ugrp.alevel) {
        vgrp.update(s => s.copy(ugrps = s.ugrps + ugrp))
      }
      if (vgrp.alevel <= ugrp.alevel) {
        ugrp.update(s => s.copy(vgrps = s.vgrps + vgrp))
      }
      if (vgrp != Alt.Grp.Base) {
        vgrp.update(s => s.copy(chkdeps = s.chkdeps + cref))
      }
      if (ugrp != Alt.Grp.Base) {
        ugrp.update(s => s.copy(chkdeps = s.chkdeps + cref))
      }
    }

  def simplified: TC = {
    val cache = MSet.empty[Var]

    def go(c: TC): TC = c match {
      case Empty => Empty

      case c: Concrete if c.isPruned => c.prunedErr

      case c @ (_: Ok | _: Fail) => c

      case Sub(value, valt, use, ualt, chk) =>
        go(chk) match {
          case Empty                      => Ok(value, valt, use, ualt)
          case Ok(_, `valt`, _, `ualt`)   => Ok(value, valt, use, ualt)
          case Fail(_, _, _, _, tes, cfs) => Fail(value, valt, use, ualt, tes, cfs)

          case c: AllOrCombos if c.chks.forall {
                case Ok(_, `valt`, _, `ualt`) => true
                case _                        => false
              } =>
            Ok(value, valt, use, ualt)

          case chk => Sub(value, valt, use, ualt, chk)
        }

      case VAlias(value, valt, chk) =>
        VAlias(value, valt, go(chk))

      case All(cs) =>
        // if any chk fails, the whole All chk fails, so we only need to keep the
        // failures.
        val cs0 = cs.map(go)
        val fails = cs0.collect { case f: Fail => f: TC }
        if (fails.nonEmpty) All(fails) else All(cs0)

      // don't bother for now
      case Combos(cs) =>
        Combos(cs.map(go))

      case v: Var =>
        // vars simplify in place
        if (cache.add(v)) {
          v.tv.update(s => s.copy(chk = go(v.chk)))
        }
        v
    }

    go(this)
  }

  def and(other: TC): TC =
    (this, other) match {
      case (Empty, b)       => b
      case (a, Empty)       => a
      case (All(a), All(b)) => All(a ++ b)
      case (All(a), b)      => All(a + b)
      case (a, All(b))      => All(b + a)
      case (a, b) if a == b => a
      case (a, b)           => All(LSet(a, b))
    }

  def combine(other: TC): TC =
    (this, other) match {
      case (Empty, b)               => b
      case (a, Empty)               => a
      case (Combos(ac), Combos(bc)) => Combos(ac ++ bc)
      case (Combos(ac), b)          => Combos(ac + b)
      case (a, Combos(bc))          => Combos(bc + a)
      case (a, b) if a == b         => a
      case (a, b)                   => Combos(LSet(a, b))
    }

  def display = display0(flat = false)
  def displayFlat = display0(flat = true)

  private def display0(flat: Boolean): String = {
    val seen = MSet.empty[Var]
    var i = ""

    def indent(f: => String): String = {
      val i0 = i
      i = "  " + i
      val ret = f
      i = i0
      ret
    }

    final case class Tup(
      l: String,
      v: Seq[(Constraint, Alt)],
      u: Seq[(Constraint, Alt)],
      d: String = "",
      sub: Seq[Tup] = Nil)

    def rpath(p: Seq[(Constraint, Alt)]) =
      p.view.map { case (c, a) => s"$a|$c" }.mkString(" > ")

    def pad(str: String, to: Int) =
      s"$str${" " * (to - str.length)}"

    def render(tups: Iterable[Tup]): String = {
      if (tups.isEmpty) {
        return ""
      }

      val rows = Seq.newBuilder[String]
      val iter = tups.iterator.map { case Tup(l, v, u, d, sub) =>
        (s"$l:", rpath(v), rpath(u), d, sub)
      }

      while (iter.hasNext) {
        val b = Seq.newBuilder[(String, String, String, String, Iterable[Tup])]

        // pick off a group until one has subs. this results in nice aligned
        // sets of columns
        var cont = true
        while (cont && iter.hasNext) {
          val t = iter.next()
          b += t
          if (t._5.nonEmpty) cont = false
        }

        val tups1 = b.result()

        if (tups1.nonEmpty) {
          val lwidth = tups1.view.map(_._1.length).max
          val vwidth = tups1.view.map(_._2.length).max
          val uwidth = tups1.view.map(_._3.length).max

          tups1.foreach { case (l, v, u, d, sub) =>
            var prefix = pad(l, lwidth)
            if (v.nonEmpty) prefix += s" ${pad(v, vwidth)}"
            if (u.nonEmpty) prefix += s" <: ${pad(u, uwidth)}"
            if (d.nonEmpty) prefix += s" - $d"
            rows += s"$i$prefix${indent(render(sub))}"
          }
        }
      }

      s"\n${rows.result().mkString("\n")}"
    }

    def go2(c: TC): Seq[Tup] =
      c match {
        case a: AllOrCombos => go(a).sub
        case c              => Seq(go(c))
      }

    def go(c: TC): Tup = c match {
      case Empty => Tup("Empty", Nil, Nil)
      case Ok(v, va, u, ua) =>
        Tup("Ok", Seq(v -> va), Seq(u -> ua))
      case Sub(v, va, u, ua, c) =>
        Tup("Sub", Seq(v -> va), Seq(u -> ua), sub = go2(c))

      case VAlias(v, va, c) =>
        Tup("VAlias", Seq(v -> va), Nil, sub = go2(c))

      case f @ Fail(v, va, u, ua, tes, cfs) =>
        val label = if (f.isFatal) "FATAL" else "Fail"
        val strs = tes.view.concat(cfs).map(_.toString)
        val errs = if (strs.isEmpty) "<pruned>" else strs.mkString(", ")
        Tup(label, Seq(v -> va), Seq(u -> ua), errs)

      case a: All             => Tup("All", Nil, Nil, sub = a.chks.map(go).toSeq)
      case a: Combos if !flat => Tup("Combos", Nil, Nil, sub = a.chks.map(go).toSeq)

      case c: Combos =>
        val flattened = c.flattened.view.map {
          // FIXME: narrow type of flattened to make this not a runtime error
          case (_, _: Var | _: Combos | _: VAlias) => sys.error("unreachable")
          case (p, c: Concrete) =>
            val tup = go(c)
            tup.copy(v = p ++ tup.v)
          case (_, c) => go(c)
        }
        Tup("Combos", Nil, Nil, sub = flattened.toSeq)

      case v: Var if seen.add(v) => Tup(s"Var ${v.tv}", Nil, Nil, sub = go2(v.chk))
      case v: Var                => Tup(s"Var ${v.tv}", Nil, Nil, "...")
    }

    render(Seq(go(this))).trim
  }
}
