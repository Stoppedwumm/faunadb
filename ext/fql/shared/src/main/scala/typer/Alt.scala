package fql.typer

import fql.ast.Span
import fql.typer.TyperDebug.{ dbg, dbgEnabled, dbgFrame }
import scala.collection.immutable.{ ArraySeq, ListSet, SortedMap }
import scala.collection.mutable.{ ArrayBuffer, Map => MMap, Set => MSet, Stack }
import scala.util.control.Breaks

/** The Alts subsystem allows the Typer to validate type overloads. Simplesub
  * (and MLSub) will emit union _values_ and intersection _uses_.
  * Implementation-wise, these are two sides of the same coin, represented as
  * gathered constraints on type vars.
  *
  * An "overload" is the opposite: Either a union in use position or an
  * intersection in value position. Typing these correctly becomes much more
  * tricky: Overload variants propagate across the structure of types resulting
  * in a sort of "spooky action at a distance" that is difficult to represent as
  * normal variable constraints.
  *
  * For example, consider a function `intBool` with the type `(Int => Int) &
  * (Boolean => Boolean)`. When checking the simple eta-expansion `x =>
  * intBool(x)` the typer will generate a type variable for the parameter `x`
  * and the output of the function. Without overloads, any function expression
  * `a => b` will always infer a function type like `A => B`. However, we want
  * our example to infer an intersection type (i.e. propagate the type of
  * intBool).
  *
  * # Alt generation
  *
  * During constrain, when an overload is involved, the Typer will create a
  * group of Alts, one for each variant of the overloaded type, and recurse for
  * each Alt. Type variable value and use constraints record which Alt they are
  * associated with.
  *
  * If multiple overloads are involved in the same expression, the product of
  * Alts is checked through the process of walking variable constraints. In
  * constrain(), the value and use types each have an associated Alt, and Alts
  * accrue value and use relationships with other alts similar to how variables
  * accrue constraints.
  *
  * Alts are leveled similar to variables. Higher level alts are contained in
  * lower level alts. In constrain() the use Alt is marked as "grounded" if the
  * value Alt has a lower level. If the value and use are not in conflict, then
  * subsequent constrain() calls consider the use Alt "required".
  *
  * # Constraint failure recovery
  *
  * Normally, any constraint failures are fatal. However, the presence of
  * overloads can allow a typecheck to still pass. If a constraint check fails,
  * the Alts involved are marked as conflicted. Failure is determined as
  * follows:
  *
  *  - Direct failure:
  *    - If the involved value Alt directly conflicts with all of the use Alt's
  *      group, the value Alt is "pruned". (There is a complication here
  *      involving consuming unions, discussed in the context of "conjoined"
  *      Alts, below.)
  *    - Transitive value Alts (i.e. other Alts which feed as values into the
  *      value Alt involved in this check) are recursively pruned according to
  *      the same predicate.
  *    - If all Alts in a group are pruned, or if a "required" Alt is pruned,
  *      the failure is fatal.
  *
  *  - Transitive failure:
  *    - The graph of alt relationships is walked to find all transient uses,
  *      and then the validPathsCheck() algorithm is used to check that:
  *      - All Alt groups have at least one valid path to associated terminal
  *        use Alts.
  *      - All "required" Alts have a valid path to associated terminal use
  *        Alts.
  *      - Whether or not any valid path "conjoins" Alts in a group.
  *    - If the valid path check above fails, the failure is fatal.
  *    - If any involved Alt group is now "conjoined", but is also "exclusive",
  *      then the failure is fatal.
  *
  * # Conjoined Alts and Alt group exclusivity.
  *
  * In certain cases, multiple Alts within the same group are required to
  * satisfy a constraint check. This can either happen if an inferred type
  * contains a union value or intersection use, or if a >1 arity function is
  * passed values handled by separate Alts. Two simple examples: `intBool(if
  * (true) 1 else true)` or `intIntBoolBool(1, true)`.
  *
  * In the first case, the type `1 | true` is inferred for the if-expr in the
  * base Alt. Each variant is checked separately against `intBool`'s input
  * types. No use Alt passes outright, but in combination they accpt all
  * possible values of the argument expression. These use Alts are therefore
  * "conjoined" in the sense that both are required for a valid constraint
  * check.
  *
  * In the `intIntBoolBool` case, both argument expressions pass with different
  * use Alts, path analysis in validPathsCheck() will discover this and mark
  * both Alts as required and the Alt group as conjoined.
  *
  * If an Alt group is conjoined as a use for any value Alt, and the use Alt is
  * marked as exclusive, then the constraint check must fail. An Alt group is
  * exclusive if any variant has an arity greater than one, which means it is
  * unsound to unify multiple Alts.
  *
  * Consider our `intBool` vs `intIntBoolBool`. `intBool` has the type `(Int =>
  * Int) & (Boolean => Boolean)` and this is a subtype of `Int | Boolean => Int
  * | Boolean`. In other words, it can safely handle an expression which may
  * result in an Int or Boolean at runtime.
  *
  * The same is not true for `intIntBoolBool`. If its first argument is an Int,
  * then its second argument must also be an Int. It is _not_ a subtype of `(Int
  * | Boolean, Int | Boolean) => Int | Boolean`. Therefore an expression like
  * `intIntBoolBool(if (...) 1 else true, if (...) 1 else true)` must be rejected.
  *
  * # Annealing
  *
  * An inferred type, if involving overloads, will have one or more associated
  * Alts, which are linked together in a graph of valid Alt combinations, from
  * input Alts through to terminal output Alts. (Similar to type variables, Alts
  * consume values in a use context and are subsequently passed in value context
  * to further uses.)
  *
  * As a final step of `typeExpr` or in the creation of polymorphic values via
  * `let` or part of an environment definition, an overloaded type is rewritten
  * via the algorithm `annealConstraint` into a flattened set of Alts belonging
  * to a single group. The resulting type is then more straightforward to
  * simplify or instantiate (in the case of `let` polymorphism).
  *
  * [note: In typing `let` statements and value inference in env typechecking,
  * annealing is currently done at the point where a polymorphic typescheme is
  * created. However, it could instead be done at the moment of instantiating
  * the polymorphic type, since annealing uses `freshen()` as its final step.]
  */
object Alt {

  lazy val Base = Grp.Base.childAlts.head

  def link(valt: Alt, ualt: Alt, undo: MMap[Alt.Grp, Alt.State] = null): Unit = {
    if (valt == ualt || valt.uses.contains(ualt) || ualt.values.contains(valt)) {
      return ()
    }

    if (valt.alevel >= ualt.alevel) {
      if (undo ne null) undo.getOrElseUpdate(valt.outerGrp, valt.outerGrp._state)
      valt.addUse(ualt)
    }
    if (valt.alevel <= ualt.alevel) {
      if (undo ne null) undo.getOrElseUpdate(ualt.outerGrp, ualt.outerGrp._state)
      ualt.addValue(valt)
    }
  }

  def fail(
    value: Constraint.Value,
    valt: Alt,
    use: Constraint.Use,
    ualt: Alt,
    sp: Span,
    fail: TypeConstraintFailure,
    undo: MMap[Alt.Grp, Alt.State])(
    implicit dbgCtx: TyperDebugCtx): Seq[TypeConstraintFailure] =
    if (!fail0(valt, ualt, sp, value, fail, undo)) {
      Nil
    } else {
      Alt.relatedFailures(valt, ualt, sp) match {
        // If fail0 returns true (failed), but related failures is empty, that
        // means an alt was exclusive and conjoined, and thefore invalid. Add in
        // a failure for this case.
        //
        // FIXME: provide a better exception here, related to
        // conjoined-ness.
        case Seq()   => Seq(TypeConstraintFailure.NotSubtype(value, use, sp))
        case related => related
      }
    }

  /** Returns true if the failure is fatal */
  private def fail0(
    valt: Alt,
    ualt: Alt,
    sp: Span,
    value: Constraint.Value,
    fail: TypeConstraintFailure,
    undo: MMap[Alt.Grp, Alt.State])(implicit dbgCtx: TyperDebugCtx): Boolean = {

    if (valt == ualt) {
      true
    } else {

      if (valt.alevel >= ualt.alevel) {
        undo.getOrElseUpdate(valt.outerGrp, valt.outerGrp._state)
        valt.addUseConflict(ualt, sp, value, fail)
      }
      if (valt.alevel <= ualt.alevel) {
        undo.getOrElseUpdate(ualt.outerGrp, ualt.outerGrp._state)
        ualt.addValConflict(valt, sp, value, fail)
      }

      if (dbgEnabled) {
        dbg(s"CONFLICT $valt -> $ualt $fail")
      }

      var fatal = false

      // direct failure
      val failed = ualt.outerGrp.liveAlts.forall(hasConflict(valt, _))
      if (failed) dbg(s"FAILED $valt ${ualt.outerGrp}")

      // TODO: mark related failures so they don't get reported if the
      // valt hasn't failed
      // squelchRelatedFails(valt, ualt.grp, sp, value)

      // prune dependencies
      if (failed) {
        if (valt.alevel < ualt.alevel) {
          fatal = true
        } else {
          val toPrune = Stack(valt)

          while (!fatal && toPrune.nonEmpty) {
            val a = toPrune.pop()
            undo.getOrElseUpdate(a.outerGrp, a.outerGrp._state)
            a.markPruned()
            dbg(s" - PRUNED $a -> ${a.validValues}")
            fatal ||= a.outerGrp.leafAlts.forall(_.isPruned)
            if (fatal) dbg(s"INVALID GROUP $a")

            toPrune ++= a.validValues.filter { a0 =>
              // doesn't conflict the other direction (FIXME: _seeeems_
              // like this shoudn't be necessary)
              !Alt.hasConflict(a, a0) &&
              // don't affect higher levels
              (a0.alevel >= a.alevel)
            }
          }
        }
      }

      // check path validity. we need to recursively check all transitive uses,
      // in case one ends up being conjoined when it shouldn't be.
      //
      // TODO: this could be a bit more efficient. We don't need to check
      // every alt group, just the terminal uses.
      if (!fatal) {
        val seen = MSet.empty[Alt.Grp]
        val stack = Stack.empty[Alt.Grp]

        def add(grp: Alt.Grp) =
          if (!seen(grp)) {
            seen += grp
            stack += grp
          }

        if (valt.alevel >= ualt.alevel) add(valt.outerGrp)
        if (valt.alevel <= ualt.alevel) add(ualt.outerGrp)

        while (!fatal && stack.nonEmpty) {
          val grp = stack.pop()
          val check = validPathsCheck(grp)

          check match {
            case Invalid =>
              dbg(s"INVALID PATHS $grp")
              fatal = true
            case Conjoined if grp.isExclusive =>
              dbg(s"INVALID CONJOINED $grp")
              fatal = true
            case _ =>
              grp.liveAlts.foreach(_.validUses.foreach(u => add(u.outerGrp)))
          }
        }
      }

      fatal
    }
  }

  sealed trait IsValid
  final case object Invalid extends IsValid
  final case object Valid extends IsValid
  final case object Conjoined extends IsValid

  private val altbreaks = new Breaks

  def validPathsCheck(grp: Alt.Grp)(implicit dbgCtx: TyperDebugCtx): IsValid = {
    import altbreaks.{ break, breakable }

    var ret: IsValid = Valid

    @inline def done(r: IsValid): Unit = {
      ret = r
      break()
    }

    breakable {
      val (required, grps) = grp.transitiveRequirements
      val pset = grp.valuePaths

      required.foreach { r =>
        val ps = pset.pathsFrom(r)
        dbg(s"REQ PATH $r -> $ps")
        if (ps.isEmpty) {
          dbg(s"Invalid: $r path conflict")
          done(Invalid)
        }
      }

      val grpPaths = OrSet.MinimalSet()

      grps.foreach { g =>
        val ps = pset.pathsFrom(g)
        dbg(s"REQ PATH $g -> $ps")
        grpPaths += OrSet(ps)
        if (ps.isEmpty) {
          dbg(s"Invalid: $g path conflict")
          done(Invalid)
        }
      }

      val grpmin = grpPaths.minimal

      dbg(s"GRP PATHS MIN $grpmin")

      if (grpmin.sizeIs > 1) {
        dbg(s"GRPS CONJOINED")
        done(Conjoined)
      }

      val base = pset.baseSets.values
      dbg(s"BASE $base")

      if (OrSet.MinimalSet(base).minimal.sizeIs > 1) {
        dbg(s"BASE CONJOINED")
        done(Conjoined)
      }

    }

    ret
  }

  // FIXME: this does not work correctly with inner/sub-alts
  def hasConflict(valt: Alt, ualt: Alt): Boolean = {
    // is there a basic recorded conflict
    def basic = if (valt.alevel >= ualt.alevel) {
      valt.useConflicts.contains(ualt)
    } else {
      ualt.valConflicts.contains(valt)
    }

    basic && !conjoined(valt, ualt)
  }

  def conjoined(valt: Alt, ualt: Alt): Boolean =
    ualt.outerGrp.conjoinedValues.getOrElse(valt, Set.empty).contains(ualt)

  def relatedFailures(valt: Alt, ualt: Alt, sp: Span): Seq[TypeConstraintFailure] = {
    val cfs = failures(valt.outerGrp, ualt.outerGrp, Some(sp))

    if (valt.outerGrp.isFailed) {
      cfs.collect {
        case (va, `ualt`, _, tf) if Alt.hasConflict(va, ualt) => tf
      }.toSeq
    } else {
      cfs.collect {
        case (`valt`, ua, _, tf) if Alt.hasConflict(valt, ua) => tf
      }.toSeq
    }
  }

  private def failures(vg: Alt.Grp, ug: Alt.Grp, sp: Option[Span])
    : Iterator[(Alt, Alt, Constraint.Value, TypeConstraintFailure)] =
    if (vg.alevel >= ug.alevel) {
      vg._state.fails.iterator
        .collect {
          case (va, ua, sp0, v, tf)
              if ua.outerGrp == ug && sp.getOrElse(sp0) == sp0 =>
            (va, ua, v, tf)
        }
    } else {
      ug._state.fails.iterator
        .collect {
          case (va, ua, sp0, v, tf)
              if va.outerGrp == vg && sp.getOrElse(sp0) == sp0 =>
            (va, ua, v, tf)
        }
    }

  // aux type defs

  final case class Level(toInt: Int) extends AnyVal with Ordered[Level] {
    def compare(o: Level) = java.lang.Integer.compare(toInt, o.toInt)
    def incr = Level(toInt + 1)

  }
  object Level {
    val Zero = Level(0)
  }

  object GrpId {
    val Base = GrpId(0)
  }
  final case class GrpId(toInt: Int) extends AnyVal {
    override def toString = s"@$toInt"
  }

  final case class State(
    childGrps: SortedMap[Alt, Set[Alt.Grp]], // child groups of the same level.
    pruned: Set[Alt], // failed
    grounded: Set[Alt], // depended on by higher level alt
    value: SortedMap[Alt, Set[Alt]], // this as ualt -> valts
    vconflicts: SortedMap[Alt, Set[Alt]], // this as ualt -> valts
    use: SortedMap[Alt, Set[Alt]], // this as valt -> ualts
    uconflicts: SortedMap[Alt, Set[Alt]], // this as valt -> ualts
    extrudes: Map[(Type.Level, Alt.Grp), Alt.Grp],
    fails: ArraySeq[(Alt, Alt, Span, Constraint.Value, TypeConstraintFailure)],
    // new state for structured typechecks
    // TODO: reconcile with above
    pruned2: ListSet[Alt], // failed
    vgrps: ListSet[Alt.Grp], // this as ugrp's vgrps
    ugrps: ListSet[Alt.Grp], // this as vgrp's ugrps
    depMap: Map[Alt, Set[Set[Alt]]], // valt -> these ualts
    chkdeps: ListSet[TypeConstraintCheck.Ref])
  object State {
    val Empty =
      State(
        SortedMap(),
        Set(),
        Set(),
        SortedMap(),
        SortedMap(),
        SortedMap(),
        SortedMap(),
        Map(),
        ArraySeq(),
        ListSet(),
        ListSet(),
        ListSet(),
        Map(),
        ListSet())
  }

  object Grp {
    val Base = Grp(GrpId.Base, 1, true, None, Level(-1), Type.Level(-1))
  }
  final case class Grp(
    id: GrpId,
    size: Int,
    isExclusive: Boolean,
    parent: Option[Alt],
    alevel: Level,
    vlevel: Type.Level) {

    val childAlts = (0 until size).view.map(Alt(_, this)).to(ArraySeq)

    var _state = State.Empty
    def update(f: State => State) = {
      // subgroups should have an empty state
      assert(isOuterGrp, "Cannot update subgroup state")
      // Base group should never be updated
      assert(this ne Grp.Base, "Cannot update Grp.Base state")

      _state = f(_state)
    }

    // TODO: cache and invalidate when a child alt is added
    def leafAlts: ArraySeq[Alt] = childAlts.flatMap { a =>
      outerGrp._state.childGrps.get(a) match {
        case Some(g) => g.flatMap(_.leafAlts)
        case None    => ArraySeq(a)
      }
    }

    def liveAlts = leafAlts.filterNot(_.isPruned)
    def liveAlts2 = leafAlts.filterNot(_.isPruned2)
    def extrudes = _state.extrudes

    // FIXME: check parent failures?
    def isFailed = _state.pruned.sizeIs == size

    def container: Option[Alt] =
      parent.flatMap(p => if (p.alevel < alevel) Some(p) else p.parent.container)

    private[Alt] def outer: Option[Alt] = parent.filter(_.alevel == alevel)
    def outerGrp: Alt.Grp = outer.fold(this)(_.parent.outerGrp)
    def isOuterGrp = outer.isEmpty

    /** Returns all Grps which directly or transitively use this one. */
    def transitiveUseGrps: Iterable[Alt.Grp] = {
      assert(isOuterGrp)

      val seen = MSet.empty[Alt.Grp]
      val stack = Stack(this)

      while (stack.nonEmpty) {
        val g = stack.pop()
        if (g != Alt.Grp.Base && seen.add(g)) {
          stack ++= g._state.ugrps
        }
      }

      seen
    }

    /** A map of transitive value Alts to sets of required Alts in this Grp, as a
      * Set of Set of Alts: At least one Alt in each Set of Alts is required. An
      * Alt is conjoined in this Grp if `depMap(valt).reduceLeft(_ & _).isEmpty`
      */
    def transitiveDepMap: Map[Alt, Set[Set[Alt]]] = {
      def merge(m1: Map[Alt, Set[Set[Alt]]], m2: Map[Alt, Set[Set[Alt]]]) = {
        m2 ++ m1.map { case (a, ss) =>
          a -> (m2.getOrElse(a, Set.empty) ++ ss)
        }
      }

      def go(grp: Alt.Grp, seen: Set[Alt.Grp]): Map[Alt, Set[Set[Alt]]] = {
        val thisMap = grp._state.depMap

        // generate a merged depMap for the key Alts of our depMap
        lazy val vDepMap = thisMap.keySet
          .map(_.outerGrp)
          .map(go(_, seen + grp))
          .reduceLeftOption(merge)
          .getOrElse(Map.empty)

        // generate a depMap by joining vDepMap -> thisMap
        lazy val transitiveMap = vDepMap
          .flatMap { case (a, vdeps) =>
            // This is pretty gross. First, we filter vdeps to only those
            // or-sets which are entirely covered by thisMap. Rationale: If an
            // Alt isn't a key in this map, then it's irrelevant and shouldn't
            // affect conjoinedness. Second, we translate vdeps to a set of deps
            // in this Grp by taking a sort of product of `vdeps` to `thisMap`.
            //
            // For example:
            // let vdeps = Set(Set(1.0), Set(1.1)) // conjoined
            // let thisMap = Map(1.0 -> Set(Set(2.0, 2.1)), 1.1 -> Set(Set(2.1)))
            //
            // 1. map vdeps to thisMap lookups
            //   Set(Set(Set(Set(2.0, 2.1))), Set(Set(Set(2.1))))
            // 2. reduce the above by multiplying out each alt sset
            //   Set(Set(2.0, 2.1), Set(2.0)) // no longer conjoined
            val map0 = vdeps.filter(_.forall(thisMap.contains)).flatMap {
              _.map(thisMap).reduceLeft { (ss1: Set[Set[Alt]], ss2: Set[Set[Alt]]) =>
                ss1.foldLeft(Set.empty[Set[Alt]]) { (ss, s1) =>
                  ss | ss2.map(_ | s1)
                }
              }
            }
            Option.when(map0.nonEmpty)(a -> map0)
          }

        if (seen(grp)) thisMap else merge(thisMap, transitiveMap)
      }

      go(this, Set.empty)
    }

    /** Returns a set of individual required Alts, as well as all other involved
      * Alt.Grps. A typecheck is valid with regards to `this` Alt.Grp if there
      * are paths leading back from each of these returned values.
      */
    def transitiveRequirements: (Set[Alt], Set[Alt.Grp]) = {
      assert(isOuterGrp)

      var required = Set.empty[Alt]
      var grps = Set.empty[Alt.Grp]

      val cache = MSet.empty[Alt.Grp]

      def transitive0(grp: Alt.Grp): Unit =
        if (cache.add(grp)) {
          grps += grp
          grp.leafAlts.foreach { a =>
            // A specific alt is required if is "grounded" (see
            // "isLeaf"/markGrounded use in TyperConstrain), and it has a valid
            // use of a value of a lower alt level.
            if (a.isGrounded && a.validValues.exists(_.alevel < a.alevel)) {
              required += a
            }

            a.values.map(_.outerGrp).foreach { g =>
              if (g.alevel >= a.alevel) {
                transitive0(g)
              }
            }
          }
        }

      transitive0(this)
      required.foreach(a => grps -= a.outerGrp)

      (required, grps)
    }

    // TODO: cache this state since it seems expensive to compute
    // returns a map of value alts to conjoined uses in this grp.
    def conjoinedValues: Map[Alt, Set[Alt]] = {
      assert(isOuterGrp)

      val vals = MMap.empty[Alt, MSet[Constraint.Value]]
      val cnjMap = MMap.empty[Alt, MMap[Alt, MSet[Constraint.Value]]]

      _state.fails.foreach { case (va, ua, _, v, tf @ _) =>
        if (ua.outerGrp == this) {
          vals.getOrElseUpdate(va, MSet.empty) += v
          cnjMap
            .getOrElseUpdate(va, MMap.empty)
            .getOrElseUpdate(ua, MSet.empty) += v
        }
      }

      vals.foreach { case (va, vals0) =>
        val map0 = cnjMap(va)

        // `va` should not have a value which failed entirely
        if (vals0.exists(v => map0.values.forall(_.contains(v)))) {
          cnjMap -= va

        } else {
          // exclude use alts which rejected all values
          leafAlts.foreach { ua =>
            if (map0.get(ua) == Some(vals0)) map0 -= ua
          }
        }
      }

      // cnjMap has been pruned appropriately
      cnjMap.view.mapValues(_.keys.toSet).toMap
    }

    /** Returns a set of valid paths from input Alts and required/based values to
      * possibly conjoined Alts in this group. See PathSet below.
      */
    // TODO: cache this state since it seems expensive to compute
    // returns set of paths, use Alts to values which feed them
    def valuePaths(implicit dbgCtx: TyperDebugCtx): PathSet = {
      assert(isOuterGrp)

      type Elem = (Either[Alt.Grp, Alt], ListSet[Alt])

      val cache = MSet.empty[Alt.Grp]

      def paths0(grp: Alt.Grp): ListSet[Elem] = {
        if (!cache.add(grp)) {
          return grp.liveAlts.view.map(ua => (Right(ua) -> ListSet(ua))).to(ListSet)
        }

        val valToUses = {
          val m = MMap.empty[Alt, MSet[Alt]]
          def add(v: Alt, u: Alt) = m.getOrElseUpdate(v, MSet.empty) += u
          val ualts = grp.liveAlts
          val vgs = ualts.view.flatMap(_.values.view.map(_.outerGrp)).to(ListSet)
          val valts = vgs.view.flatMap(_.leafAlts)

          // HACK: during constrain, we incrementally build up links plus
          // conflicts. The ends up linking all use alts to all value alts, and
          // conflicts for incompats.

          // The below handles the case where alts eventually get linked up by
          // taking the product and subtracting out current conflicts.

          // However, in other cases (linking free alts, extrusion) there never
          // will be conflicts, and the correct paths are indicated _only_ by
          // links. The data structure needs to be cleaned up, and then this
          // code can be simplified.
          var hasConflicts = false

          ualts.foreach { u =>
            valts.foreach { v =>
              if (!v.isPruned) {
                if (Alt.hasConflict(v, u)) {
                  hasConflicts = true
                } else {
                  add(v, u)
                }
              }
            }
          }

          // fall back to just links
          if (!hasConflicts) {
            m.clear()
            ualts.foreach(u => u.validValues.foreach(add(_, u)))
          }

          m
        }

        val cnjd = grp.conjoinedValues
        val valPaths = valToUses.keySet.map(_.outerGrp).flatMap(paths0)

        dbg(s"$grp")
        dbg(s"  VALUES $valToUses")
        dbg(s"  PATHS  $valPaths")
        dbg(s"  CNJD   $cnjd")

        val ret = ListSet.newBuilder[Elem]

        valPaths.foreach { case (root, vp) =>
          // The container alt is special. whereas in-level alts are
          // instantiated once and unified, the base alt is not. Therefore we
          // disambiguate based on the use group.
          val root0 = root match {
            case Right(r) if r.alevel < grp.alevel =>
              Left(grp)
            case r => r
          }

          val combos = vp.foldLeft(ListSet(ListSet.empty[Alt])) { (sets, v) =>
            val uas = valToUses.getOrElse(v, Set.empty)
            val c = cnjd.getOrElse(v, Set.empty)
            sets.flatMap(s => uas.map(s ++ c + _))
          }

          combos.foreach { c =>
            dbg(s"   - $root0 -> $c")
            ret += (root0 -> c)
          }
        }

        grp.liveAlts.foreach { ua =>
          dbg(s"   - ${Right(ua)} -> ${Set(ua)}")
          ret += (Right(ua) -> ListSet(ua))
        }

        ret.result()
      }

      dbgFrame(s"Value paths $this") {
        PathSet(paths0(this))
      }
    }

    override def equals(o: scala.Any) = o match {
      case o: Grp => id == o.id
      case _      => false
    }

    override def hashCode() = id.hashCode()

    override def toString = {
      val p = parent.filter(_.alevel == alevel).fold("")(p => s"$p/")
      val l = "`" * alevel.toInt
      val vl = "'" * vlevel.toInt
      s"$p@${id.toInt}$l$vl"
    }
  }

  /** An "OrSet" is a set of alternative sets of conjoined alts. In usage, an
    *  OrSet is satisfied if _any_ of its conjoined sets is satisfied. A
    *  conjoined set is satisfied if _all_ of its alts are satisfied.
    */
  final case class OrSet(toSet: Set[ListSet[Alt]]) extends AnyVal {
    override def toString = s"Or(${toSet.mkString(", ")})"
  }
  object OrSet {

    /** MinimalSet is a builder like data-structure that tracks the minimal set among
      * multiple OrSet instances.
      */
    final class MinimalSet private (
      val minimalSets: ArrayBuffer[ListSet[Alt]] = ArrayBuffer.empty)
        extends AnyVal {

      @inline def ++=(ors: Iterable[OrSet]): this.type = {
        ors foreach +=
        this
      }

      def +=(or: OrSet): this.type = {
        val alts = or.toSet
        if (alts.nonEmpty) {
          if (minimalSets.isEmpty) {
            minimalSets ++= alts
          } else {
            // Produces sets which captures the alt constraints of both existing
            // minimal sets and the new OrSet provided.
            minimalSets.flatMapInPlace(cjd => alts.map(cjd | _))
          }
          // Prune the list of minimal sets based on the given assumptions:
          //
          // 1. The union of sets can only produce new sets of equal size or bigger;
          //
          // 2. The union of a bigger set in the list can never produce a new set
          //    smaller than the union of any of the smaller sets in the list.
          //
          // These as assumptions allow to prune the list of minimal sets by
          // preserving only the smallest ones.
          val minSize = minimalSets.view.map(_.size).min
          minimalSets.filterInPlace(_.sizeIs == minSize)
        }
        this
      }

      /** Return the smallest satisfying alt set. If the size is > 1, the Alt.Grps
        * involved must be conjoined.
        */
      def minimal: ListSet[Alt] =
        minimalSets.headOption.getOrElse(ListSet.empty)

      /** Return the smallest satisfying alt set unified with the given alts. If the
        * size is > 1, the Alt.Grps involved must be conjoined.
        *
        * Note that the given `alts` is not added to the list of minimal sets.
        */
      def minimalWith(alts: ListSet[Alt]): ListSet[Alt] =
        minimalSets.map(_ | alts).minByOption(_.size).getOrElse(alts)
    }
    object MinimalSet {
      def apply() = new MinimalSet()
      def apply(ors: Iterable[OrSet]) = new MinimalSet() ++= ors
    }
  }

  /** A "PathSet" is a set/multi-map of all possible valid paths from inputs (all
    * transitive dependency Alts as well as base uses) to conjoined Alts.
    * Multiple entries represent the presence of alternative ways for the type
    * to satisfy the Alt dependency.
    *
    * # Examples
    *
    * It's also worth playing around with debug output in typer tests, which
    * will output value paths for Alt groups in a similar format as below.
    *
    * Paths marked with `+` end up being chosen as part of annealing.
    *
    * Example 1: `x => intBool(x)`
    * Alt.Grps:        1.(0,1)
    * Paths:
    *   + Right(1.0) -> Set(1.0) // Alts always feed into themselves
    *   + Right(1.1) -> Set(1.1)
    *
    * Example 2: `intBoolStr(if (true) 1 else true)`
    * Alt.Grps:   1.(0,1,2)            0.0    0.0
    * Paths:
    *   + Left(1) -> Set(1.0, 1.1) // `Left` keys represent base use in the
    *                              // Alt.Grp. Two alts are involved meaining
    *                              // they are conjoined.
    *   - Right(1.0) -> Set(1.0)
    *   - Right(1.1) -> Set(1.1)
    *   - Right(1.2) -> Set(1.2)
    *
    * Example 3: `x => strBool(intBoolStr(x))`
    * Alt.Grps:        2(0,1)  1(0,1,2)
    * Paths:
    *   + Right(1.1) -> Set(2.0) // Note: 1.0 is missing because there it does
    *                            // not have a valid value->use path and is pruned.
    *   + Right(1.2) -> Set(2.1)
    *   - Right(2.0) -> Set(2.0)
    *   - Right(2.1) -> Set(2.1)
    *
    * Example 4: `intBoolStr(if (true) intBool(if (true) 1 else true) else 'foo')`
    * Alt.Grps:   2.(0,1,2)            1(0,1)            0.0    0.0        0.0
    * Paths:
    *   + Left(1)    -> Set(2.0, 2.1) // base values (1, true) consumed by Alt.Grp 1
    *   - Right(1.0) -> Set(2.0)
    *   - Right(1.1) -> Set(2.1)
    *   + Left(2)    -> Set(2.2) // base values ('foo') consumed by Alt.Grp 2
    *   - Right(2.0) -> Set(2.0)
    *   - Right(2.1) -> Set(2.1)
    *   - Right(2.2) -> Set(2.2)
    *
    * Example 5: `x => intBool(intBool(intBool(x)))`
    * Alt.Grps:        3       2       1
    * Paths:
    *   + Right(1.0) -> Set(3.0)
    *   + Right(1.1) -> Set(3.1)
    *   - Right(2.0) -> Set(3.0)
    *   - Right(2.1) -> Set(3.1)
    *   - Right(3.0) -> Set(3.0)
    *   - Right(3.1) -> Set(3.1)
    */
  // FIXME: convert to some sort of Map[Either[...], OrSet]
  final case class PathSet(toSet: ListSet[(Either[Alt.Grp, Alt], ListSet[Alt])])
      extends AnyVal {

    def |(other: PathSet) = PathSet(this.toSet | other.toSet)

    def pathsFrom(dep: Alt) =
      toSet.collect { case (Right(`dep`), alts) => alts }

    def pathsFrom(dep: Alt.Grp) =
      toSet.collect { case (Right(a), alts) if a.outerGrp == dep => alts }

    def depSets =
      toSet.view.collect { case (Right(d), alts) => d -> alts }

    def baseSets: Map[Alt.Grp, OrSet] =
      toSet.view
        .collect { case (Left(d), alts) => d -> alts }
        .groupBy(_._1)
        .map { case (g, ps) => g -> OrSet(ps.map(_._2).toSet) }

    def unifiedSets: (Map[Alt, OrSet], ListSet[Alt]) = {
      val bases = OrSet.MinimalSet(baseSets.values)

      val deps = depSets
        .map { case (d, alts) => d -> bases.minimalWith(alts) }
        .groupBy(_._1)
        .view
        .mapValues(vs => OrSet(vs.map(_._2).toSet))
        .toMap

      (deps, bases.minimal)
    }
  }
}

final case class Alt(idx: Int, parent: Alt.Grp) extends Ordered[Alt] {

  def outerGrp = parent.outerGrp
  def lineage: Set[Alt] = parent.outer.fold(Set.empty[Alt])(_.lineage) + this

  private def state = parent.outerGrp._state
  private def update(f: Alt.State => Alt.State) = parent.outerGrp.update(f)

  def isPruned = lineage.exists(state.pruned.contains)
  def isPruned2 = lineage.exists(state.pruned2.contains)
  def isGrounded = lineage.exists(state.grounded.contains)

  def markPruned() = update(s => s.copy(pruned = s.pruned + this))
  def markPruned2() = update(s => s.copy(pruned2 = s.pruned2 + this))
  def markGrounded() = update(s => s.copy(grounded = s.grounded + this))

  def alevel = parent.alevel
  def vlevel = parent.vlevel
  def childGrps: Set[Alt.Grp] = state.childGrps.getOrElse(this, Set.empty)

  def values = state.value.getOrElse(this, Set.empty)
  def uses = state.use.getOrElse(this, Set.empty)
  def valConflicts = state.vconflicts.getOrElse(this, Set.empty)
  def useConflicts = state.uconflicts.getOrElse(this, Set.empty)
  def validValues = values.filterNot(a => a.isPruned || Alt.hasConflict(a, this))
  def validUses = uses.filterNot(a => a.isPruned || Alt.hasConflict(this, a))

  def addChildGrp(g: Alt.Grp) = {
    // null out subgroup state so that things blow up if it is accessed
    assert(g._state == Alt.State.Empty)
    g._state = null

    assert(g.alevel == alevel, s"Grp level mismatch? $parent $g")
    assert(g.parent == Some(this), s"Not parent? ${g.parent} $this")
    update(s => s.copy(childGrps = s.childGrps + (this -> (childGrps + g))))
  }

  def addValue(a: Alt) = {
    assert(a.alevel <= alevel)
    assert(a != this)
    update(s => s.copy(value = s.value + (this -> (values + a))))
  }

  def addValConflict(
    a: Alt,
    sp: Span,
    v: Constraint.Value,
    fail: TypeConstraintFailure) = {
    assert(values contains a)
    update(s =>
      s.copy(
        vconflicts = s.vconflicts + (this -> (valConflicts + a)),
        fails = s.fails :+ ((a, this, sp, v, fail))))
  }

  def addUse(a: Alt) = {
    assert(a.alevel <= alevel)
    assert(a != this)
    update(s => s.copy(use = s.use + (this -> (uses + a))))
  }

  def addUseConflict(
    a: Alt,
    sp: Span,
    v: Constraint.Value,
    fail: TypeConstraintFailure) = {
    assert(uses contains a)
    update(s =>
      s.copy(
        uconflicts = s.uconflicts + (this -> (useConflicts + a)),
        fails = s.fails :+ ((this, a, sp, v, fail))))
  }

  def compare(o: Alt) =
    java.lang.Integer.compare(parent.id.toInt, o.parent.id.toInt) match {
      case 0   => java.lang.Integer.compare(idx, o.idx)
      case cmp => cmp
    }

  override def toString = {
    val p = if (isPruned2) "~" else ""
    s"$p$parent.$idx$p"
  }
}
