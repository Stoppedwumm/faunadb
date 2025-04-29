package fql.typer

import fql.ast.{ Literal, Span }
import fql.error.TypeError
import fql.typer.Constraint.{ Use, Value }
import java.util.concurrent.TimeoutException
import scala.collection.immutable.{ ArraySeq => ASeq }
import scala.collection.mutable.{ Buffer, Map => MMap, Set => MSet }
import scala.concurrent.duration._

trait TyperConstrain { self: Typer =>

  protected def constrain(value: Value, use: Use, callSite: Span)(
    implicit vctx: Typer.VarCtx,
    containerAlt: Alt,
    level: Type.Level,
    deadline: Deadline = Int.MaxValue.seconds.fromNow): Typecheck[Unit] = {
    dbg(s"CONSTRAIN at $callSite")

    val cs = new CState(containerAlt, level, callSite, deadline)
    cs.run(value, use)
    cs.check.runCheck()

    val (fails, fatals) = cs.check.errors

    if (cs.check.isFailed) {
      // put original var/alt bounds back so cfailures render correctly
      cs.undoState()
    }

    // FIXME: this seems like this distinct() shouldn't be necessary if we can
    // massage failures in Alt.fail
    val errors = fails match {
      case Seq() => fatals
      case cfs   =>
        // In order to display a Type, we need to get the set of Alts associated with
        // free variables, since we might be rendering a subexpression of a larger
        // expression. The set of free Alts is then passed to anneal() to ensure they
        // are activated correctly.
        val free = freeAlts(vctx)(rootAlt)

        def dispVal(v: Constraint.Value) =
          displayValue(annealValue(v, containerAlt, Type.Level.Zero, free))
        def dispUse(u: Constraint.Use) =
          displayUse(annealUse(u, containerAlt, Type.Level.Zero, free))

        val bundled = TypeConstraintFailure
          .toTypeError(value, use, callSite, cfs)(dispVal, dispUse)

        fatals :+ bundled
    }

    Typecheck((), errs = errors.toList)
  }

  protected def canConstrain(value: Value, use: Use)(
    implicit containerAlt: Alt,
    deadline: Deadline = Int.MaxValue.seconds.fromNow): Boolean = {
    dbg("CAN CONSTRAIN")

    val cs = new CState(containerAlt, Type.Level.Zero, Span.Null, deadline)
    cs.run(value, use)
    // always put back state
    cs.undoState()

    cs.errs.isEmpty && cs.cfs.isEmpty
  }

  /** A one-shot object which holds shared state across an entire constrain call.
    */
  private final class CState(
    val containerAlt: Alt,
    val vlevel: Type.Level,
    val callSite: Span,
    val deadline: Deadline) {

    val errs = Buffer.empty[TypeError]
    val cfs = Buffer.empty[TypeConstraintFailure]

    val check = new TypeConstraintCheck.Ref

    // These are used to track persisted state, to undo if necessary
    val varUndo = MMap.empty[Type.Var, Type.Var.State]
    val altUndo = MMap.empty[Alt.Grp, Alt.State]

    val constrainCache = MSet.empty[(Value, Alt, Use, Alt)]
    val typeSchemeCache =
      MMap.empty[(Span, Span, TypeScheme, Alt, Alt, Boolean), Constraint]
    val overloadAltCache = MMap.empty[(Span, Seq[Type]), Alt.Grp]

    // FIXME: fix method names...this is the top-level entry point to constrain.
    def run(value: Value, use: Use): Unit = {
      val c =
        try {
          constrain(value, containerAlt, use, containerAlt)
        } catch {
          case _: TimeoutException =>
            val err = TypeError.TimeLimitExceeded(callSite)
            errs += err
            cfs.clear()
            TypeConstraintCheck.Fail(
              value,
              containerAlt,
              use,
              containerAlt,
              ASeq(err),
              ASeq.empty)
        }

      check.set(c)
    }

    def undoState() = {
      varUndo.foreach { case (v, s) =>
        dbg(s"Undo var $v")
        v.update(_ => s)
      }
      varUndo.clear()
      altUndo.foreach { case (g, s) =>
        dbg(s"Undo altr grp $g")
        g.update(_ => s)
      }
      altUndo.clear()
    }

    def constrain(
      value: Value,
      valt: Alt,
      use: Use,
      ualt: Alt): TypeConstraintCheck =
      dbgFrame(
        s"cons0 $valt/$ualt $value <: $use (v: ${value.span}, u: ${use.span})") {

        implicit val level = vlevel

        if (deadline.isOverdue()) {
          throw new TimeoutException
        }

        def error(err: TypeError) = {
          errs += err
          TypeConstraintCheck.Fail(value, valt, use, ualt, ASeq(err), ASeq.empty)
        }

        def fail(failure: TypeConstraintFailure) = {
          // TODO: remove calls to Alt.link, Alt.fail
          val fatal = if (valt != ualt) {
            if (valt.alevel < ualt.alevel || valt.isGrounded) {
              dbg(s"Conflict (Ground) (chk) $valt -> $ualt")
            } else {
              dbg(s"Conflict (chk) $valt -> $ualt")
            }

            Alt.link(valt, ualt, altUndo)
            Alt.fail(value, valt, use, ualt, callSite, failure, altUndo)
          } else {
            Seq(failure)
          }

          if (fatal.nonEmpty) {
            if (dbgEnabled) {
              dbg("FATAL!")
              fatal.foreach(tf => dbg(s" + $tf"))
            }

            cfs ++= fatal
          }

          TypeConstraintCheck.Fail(value, valt, use, ualt, ASeq.empty, ASeq(failure))
        }

        def ok() = {
          if (valt != ualt) {
            Alt.link(valt, ualt, altUndo)
            if (valt.alevel < ualt.alevel) {
              ualt.markGrounded() // should add to undo but link() already does it
            }

            if (valt.alevel < ualt.alevel || valt.isGrounded) {
              dbg(s"Dep (Ground) (chk) $valt -> $ualt")
            } else {
              dbg(s"Dep (chk) $valt -> $ualt")
            }
          }

          TypeConstraintCheck.Ok(value, valt, use, ualt)
        }

        def sub(chk: TypeConstraintCheck) = {
          import TypeConstraintCheck._
          // FIXME let prune simplify
          if (chk == Empty) ok() else Sub(value, valt, use, ualt, chk)
        }

        def suball(chks: Iterable[TypeConstraintCheck]) =
          sub(TypeConstraintCheck.all(chks))

        def valias(chk: TypeConstraintCheck) = {
          import TypeConstraintCheck._
          assert(chk != Empty)
          VAlias(value, valt, chk)
        }

        def combos(chks: Iterable[TypeConstraintCheck]) =
          TypeConstraintCheck.combos(chks)

        // widen a wildcard type to account for the fact wc fields may not exist
        // FIXME: do this in type construction, perhaps
        def widenWildcard(wc: Type) = wc match {
          // special-case these
          case t @ (Type.Any(_) | Type.Never(_) | Type.Null) => t
          case u @ Type.Union(vs, _) => u.copy(variants = vs :+ Type.Null)
          case t                     => Type.Optional(t)
        }

        def zipSchemeParams(
          sch: TypeScheme,
          params: Iterable[String],
          args: Iterable[Constraint]) = {
          val piter = params.iterator
          val aiter = args.iterator
          val pairs = Seq.newBuilder[(String, Constraint)]

          while (piter.hasNext && aiter.hasNext) {
            pairs += (piter.next() -> aiter.next())
          }

          if (piter.hasNext || aiter.hasNext) {
            throw new IllegalStateException(
              s"Mismatched arguments for $sch: $params, $args")
          }

          pairs.result()
        }

        def instantiateSchemeV(v: Type.Named, shape: TypeShape, sch: TypeScheme)(
          implicit level: Type.Level): Constraint.Value =
          typeSchemeCache
            .getOrElseUpdate(
              (v.span, callSite, sch, valt, ualt, true),
              instantiateValue(sch, zipSchemeParams(sch, shape.tparams, v.args)))
            .asInstanceOf[Value]

        def instantiateSchemeU(u: Type.Named, shape: TypeShape, sch: TypeScheme)(
          implicit level: Type.Level): Constraint.Use =
          typeSchemeCache
            .getOrElseUpdate(
              (u.span, callSite, sch, valt, ualt, false),
              instantiateUse(sch, zipSchemeParams(sch, shape.tparams, u.args)))
            .asInstanceOf[Use]

        def instantiateAltGrp(
          variants: Seq[Type],
          exclusive: Boolean,
          parentAlt: Alt) =
          overloadAltCache.getOrElseUpdate(
            (callSite, variants), {
              val grp = if (parentAlt == containerAlt) {
                freshAltGrp(variants.size, exclusive, parentAlt, vlevel)
              } else {
                freshAltInnerGrp(variants.size, exclusive, parentAlt, vlevel)
              }
              if (dbgEnabled) {
                dbg(s"Instantiating overload $grp")
                variants.zip(grp.childAlts).foreach { case (v, valt) =>
                  dbg(s" + $valt: $v")
                }
              }
              grp
            }
          )

        def constrainProj(
          value: Value,
          proj: Value,
          ret: Use,
          wrap: Type.Var => Type): TypeConstraintCheck = {

          val chks = ASeq.newBuilder[TypeConstraintCheck]

          val a = freshVar(value.span)
          val b = freshVar(ret.span)
          // we can't put `value` directly in the projector use constraint (since
          // `value` is not a `Type`), so thread it through a var.
          chks += constrain(value, valt, a, ualt)
          // constraining the projector wires up `a` and `b` to its input and output
          chks += constrain(
            proj,
            valt,
            Type.Function(ASeq((None -> a)), None, b, value.span),
            ualt)
          // wire up the return var
          chks += constrain(wrap(b), valt, ret, ualt)

          // do not undo a and b in order to propagate constraints
          varUndo -= a
          varUndo -= b

          suball(chks.result())
        }

        (value, use) match {
          case (Constraint.Lazy(v), u) =>
            constrain(v, valt, u, ualt)

          // value and use are the same
          case _ if value eq use =>
            ok()

          // two alts in the same group shouldn't compare against each other
          case _ if valt != ualt && valt.outerGrp == ualt.outerGrp =>
            TypeConstraintCheck.Empty

          // We've already seen this Var/Type pair, which means we have recursed.
          // Short-circuit.
          case ((_: Type.Var, _) | (_, _: Type.Var))
              if !constrainCache.add((value, valt, use, ualt)) =>
            dbg(s"cached")
            TypeConstraintCheck.Empty

          /** Meta-types. The following cases deal with variables and set-types (union,
            * intersection, difference). They do not correspond to type structure.
            */

          // Variables

          case (value, use: Type.Var) if value.level <= use.level =>
            // A Union on the value side needs to be exploded to multiple value
            // bounds on the var
            value match {
              case Type.Union(vs, _) =>
                combos(vs.view.map(constrain(_, valt, use, ualt)))
              case value =>
                dbg(s"Adding value bound $valt to $use")
                val valt0 = extrudeA(valt)(use.level)
                varUndo.getOrElseUpdate(use, use._state)
                use.addValue(value, valt0)

                if (dbgEnabled) {
                  if (use.uses.nonEmpty) dbg(s"$use use bounds")
                  use.uses.foreach { case (u0, ualt0) =>
                    dbg(s" + $u0 $ualt0")
                  }
                }

                use.uses.foreach { case (u0, ualt0) =>
                  if (ualt0.isPruned) {
                    dbg(s"transitive use: $valt0 -> $ualt0 (pruned)")
                  } else {
                    dbg(s"transitive use: $valt0 -> $ualt0")
                    use.addCheck(constrain(value, valt0, u0, ualt0))
                  }
                }

                use.check
            }

          case (value: Type.Var, use) if use.level <= value.level =>
            // An Intersect on the use side needs to be exploded to multiple use
            // bounds on the var
            use match {
              case Type.Intersect(vs, _) =>
                combos(vs.view.map(constrain(value, valt, _, ualt)))
              case use =>
                dbg(s"Adding use bound $ualt to $value")
                val ualt0 = extrudeA(ualt)(value.level)
                varUndo.getOrElseUpdate(value, value._state)
                value.addUse(use, ualt0)

                if (dbgEnabled) {
                  if (value.values.nonEmpty) dbg(s"$value value bounds")
                  value.values.foreach { case (v0, valt0) =>
                    dbg(s" + $v0 $valt0")
                  }
                }

                value.values.foreach { case (v0, valt0) =>
                  if (valt0.isPruned) {
                    dbg(s"transitive use: $valt0 -> $ualt0 (pruned)")
                  } else {
                    dbg(s"transitive use: $valt0 -> $ualt0")
                    value.addCheck(constrain(v0, valt0, use, ualt0))
                  }
                }

                value.check
            }

          case (value, use: Type.Var) =>
            if (value.vars.exists(_.isRight)) {
              // TODO: this case is a sanity check against skolems leaking
              // out from let signature checks. needs more test cases to ensure it
              // cannot happen.
              throw new IllegalStateException(
                s"unexpected skolem in extrude $value $use")
            } else {
              val value0 = extrudeV(value)(use.level)
              constrain(value0, valt, use, ualt)
            }

          case (value: Type.Var, use) =>
            if (use.vars.exists(_.isRight)) {
              dbg(s"SKOLEM in extrude $value $use")
              error(
                TypeError.InvalidSkolemConstraint(
                  displayUse(use),
                  use.span,
                  value.span,
                  callSite))
            } else {
              val use0 = extrudeU(use)(value.level)
              constrain(value, valt, use0, ualt)
            }

          // Overloads ("All" cases)

          // FIXME: this duplicates the union handling logic in leveled
          // vars above, but we need this to come after extrusion, so it's
          // duplicated here.
          case (Type.Union(variants, _), _) =>
            combos(variants.view.map(constrain(_, valt, use, ualt)))

          case (_, Type.Intersect(variants, _)) =>
            combos(variants.view.map(constrain(value, valt, _, ualt)))

          // Overloads ("Any" cases)
          // We need to instantiate Alt groups in these cases.

          case (Constraint.Intersect(variants), _) =>
            combos(variants.map { case (valt, v) => constrain(v, valt, use, ualt) })

          case (value @ Type.Intersect(variants, _), _) =>
            val exclusive = typeArity(value) > 1
            val agrp = instantiateAltGrp(variants, exclusive, valt)
            val chks = variants.view
              .zip(agrp.childAlts)
              .map { case (v, valt0) =>
                dbg(s"transitive use: $valt0 -> $valt -> $ualt")
                constrain(v, valt0, use, ualt)
              }
              .toSeq
            // do not undo grp in order to propagate constraints
            altUndo -= agrp
            combos(chks)

          case (_, Constraint.Union(variants)) =>
            combos(variants.map { case (ualt, u) =>
              constrain(value, valt, u, ualt)
            })

          case (_, Type.Union(variants, _)) =>
            val agrp = instantiateAltGrp(variants, false, ualt)
            val chks = variants.view
              .zip(agrp.childAlts)
              .map { case (u, ualt0) =>
                dbg(s"transitive use: $valt -> $ualt -> $ualt0")
                constrain(value, valt, u, ualt0)
              }
              .toSeq
            // do not undo grp in order to propagate constraints
            altUndo -= agrp
            combos(chks)

          // Difference

          // These paired constraints result in the Diff constraint pushed to just on
          // top of concrete use.

          case (Constraint.Diff(v, minus), u) =>
            dbg(s"diff value: $v - $minus")
            sub(constrain(v, valt, Constraint.DiffGuard(minus, u), ualt))

          case (v, Constraint.DiffGuard(minus, u)) =>
            // Subtract out `minus` from the concrete value. This can be pretty
            // simple
            // since `minus` is only Null, currently, and we can restrict it to
            // non-aliased Type.Name types.
            //
            // Returns None if subtracting `minus` from `v` is a no-op
            def subtract(v: Value): Option[ASeq[Value]] =
              v match {
                case _: Type.Var =>
                  throw new IllegalStateException("Unexpected variable!")

                case v if v == minus => Some(ASeq.empty)

                case v: Type.Named if typeShape(v.name).alias.isDefined =>
                  val shape = typeShape(v.name)
                  val sch = shape.alias.get
                  val v0 = instantiateSchemeV(v, shape, sch)
                  dbg(s"Resolving alias for diff: [$value]($sch) -> $v0")
                  subtract(v0)

                // remove intersects if _all_ variants get dropped
                case v: Type.Intersect =>
                  val subbed = v.variants.forall { v0 =>
                    subtract(v0) match {
                      case None      => false
                      case Some(rem) => rem.isEmpty
                    }
                  }
                  Option.when(subbed)(ASeq.empty)

                // filter union variants
                case v: Type.Union =>
                  var subbed = false
                  val vb = ASeq.newBuilder[Value]
                  v.variants.foreach { v0 =>
                    subtract(v0) match {
                      case None =>
                        vb += v0
                      case Some(rem) =>
                        subbed = true
                        vb ++= rem
                    }
                  }
                  Option.when(subbed)(vb.result())

                case _ => None
              }

            subtract(v) match {
              case None =>
                sub(constrain(v, valt, u, ualt))
              case Some(vdiff) =>
                suball(vdiff.map(constrain(_, valt, u, ualt)))
            }

          /** "Regular" type matches. The following cases handle opaque or structured
            * types, and imply some concrete relationship between the value and
            * use Alts.
            */

          // Skolems, Named

          case (v: Type.Skolem, u: Type.Skolem) if !v.isInvalid && !u.isInvalid =>
            if (v.name.str == u.name.str) {
              ok()
            } else {
              val span = value.span.getOrElse(use.span).getOrElse(callSite)
              fail(TypeConstraintFailure.InvalidGenericConstraint(value, use, span))
            }
          case (s: Type.Skolem, _) =>
            // if the skolem is invalid, substitute it with a poisoned var to not
            // generate bad errors.
            if (s.isInvalid) {
              val v = freshVar(s.span)(s.level)
              v.poison()
              constrain(v, valt, use, ualt)
            } else {
              val span = use.span.getOrElse(value.span).getOrElse(callSite)
              fail(TypeConstraintFailure.InvalidGenericConstraint(value, use, span))
            }
          case (_, s: Type.Skolem) =>
            // if the skolem is invalid, substitute it with a poisoned var to not
            // generate bad errors.
            if (s.isInvalid) {
              val u = freshVar(s.span)(s.level)
              u.poison()
              constrain(value, valt, u, ualt)
            } else {
              val span = value.span.getOrElse(use.span).getOrElse(callSite)
              fail(TypeConstraintFailure.InvalidGenericConstraint(value, use, span))
            }

          case (v: Type.Named, u: Type.Named) if v.name.str == u.name.str =>
            // We assume that args seqs in both are the same length
            // FIXME: look up arg co/contra-variance and do the right thing here
            val chks = v.args.view
              .zip(u.args)
              .map { case (v, u) => constrain(v, valt, u, ualt) }
            suball(chks)

          // FIXME: we hard-code the number subtyping relationship here.
          // Generalize this pattern.
          case (Type.Int | Type.Long | Type.Double, Type.Number) |
              (Type.Int, Type.Long) |
              (Type.Double | Type.Float, Type.Double | Type.Float) =>
            ok()

          // Lits/Singletons

          case (Constraint.Lit(l0, _), use: Type.Named) if Type.isValueOf(l0, use) =>
            ok()
          case (Constraint.Lit(l0, vspan), Type.Singleton(l1, uspan)) =>
            if (l0 != l1) {
              val span = vspan.getOrElse(uspan).getOrElse(callSite)
              fail(TypeConstraintFailure.NotSameSingleton(value, use, span))
            } else {
              ok()
            }

          // Against any other types, lits should be promoted to their primary type
          case (Constraint.Lit(lit, span), _) =>
            val ptype = Type.primaryLitType(lit, span)
            // FIXME: this should not swap out the singleton for tracking
            // purposes. Fix constrain loop. maybe sub will work for now
            sub(constrain(ptype, valt, use, ualt))

          // Tuples

          case (Constraint.Tup(vs, span), Type.Tuple(us, _)) =>
            if (vs.size != us.size) {
              fail(
                TypeConstraintFailure
                  .InvalidTupleArity(value, span, us.size, vs.size))
            } else {
              val chks =
                vs.view
                  .zip(us)
                  .map { case (v, u) => constrain(v, valt, u, ualt) }
              suball(chks)
            }

          // Records

          case (
                Constraint.Rec(vfields, vwild, vspan),
                Type.Record(ufields, uwild, uspan)) =>
            val chks = ASeq.newBuilder[TypeConstraintCheck]

            chks ++= vfields.view.map { case (field, v) =>
              ufields.get(field).orElse(uwild) match {
                case Some(u) => constrain(v, valt, u, ualt)
                case None =>
                  val span = uspan.getOrElse(vspan).getOrElse(callSite)
                  fail(TypeConstraintFailure.ExtraField(value, span, field))
              }
            }

            val vwild0 = vwild.map(widenWildcard)

            chks ++= ufields.view.collect {
              case (field, u) if !vfields.contains(field) =>
                vwild0 match {
                  case Some(v)                      => constrain(v, valt, u, ualt)
                  case None if allowMissingField(u) => TypeConstraintCheck.Empty
                  case None =>
                    val span = uspan.getOrElse(vspan).getOrElse(callSite)
                    fail(
                      TypeConstraintFailure
                        .MissingField(value, callSite, span, field))
                }
            }

            chks += ((vwild, uwild) match {
              case (Some(v), Some(u)) =>
                constrain(v, valt, u, ualt)
              case (Some(_), None) =>
                val span = uspan.getOrElse(vspan).getOrElse(callSite)
                fail(TypeConstraintFailure.ExtraWildcard(value, span))
              case _ =>
                TypeConstraintCheck.Empty
            })

            suball(chks.result())

          /** Type destructuring. The following cases handle destructuring types based on
            * syntax (or an interface constraint). There are two cases each for
            * field selection, apply, and access: Once matching the corresponding
            * concrete type, and the second for virtual type-shape lookup. These
            * all imply a concrete relationship.
            */

          // Field selection

          case (
                Constraint.Rec(vfields, vwild, vspan),
                Constraint.Interface(ufield, u, dotSpan, uspan)) =>
            val vwild0 = vwild.map(widenWildcard)

            vfields.get(ufield).orElse(vwild0) match {
              case Some(v) =>
                sub(constrain(v, valt, u, ualt))
              case None =>
                val span = uspan.getOrElse(vspan)
                fail(
                  TypeConstraintFailure.MissingField(value, dotSpan, span, ufield))
            }

          case (
                Type.Interface(vfield, v, vspan),
                Constraint.Interface(ufield, u, dotSpan, uspan)) =>
            if (vfield == ufield) {
              sub(constrain(v, valt, u, ualt))
            } else {
              val span = uspan.getOrElse(vspan)
              fail(TypeConstraintFailure.MissingField(value, dotSpan, span, ufield))
            }

          case (v: Type.Named, Constraint.Interface(field, u, _, _))
              if typeShape(v.name).fields.contains(field) =>
            val shape = typeShape(v.name)
            val sch = shape.fields(field)
            val v0 = instantiateSchemeV(v, shape, sch)
            dbg(s"Resolving .$field: [$value]($sch) -> $v0")
            sub(constrain(v0, valt, u, ualt))

          // Function application

          case (
                Constraint.Func(vpars, vvariad, vret),
                Constraint.Apply(uargs, uret, argsSpan)) =>
            val span = argsSpan.getOrElse(callSite)

            vvariad match {
              case Some(_) if vpars.sizeIs > uargs.size =>
                // TODO: Improve error to say "at least ps0.size args"
                fail(
                  TypeConstraintFailure
                    .InvalidArity(value, span, vpars.size, uargs.size))
              case None if vpars.sizeIs != uargs.size =>
                fail(
                  TypeConstraintFailure
                    .InvalidArity(value, span, vpars.size, uargs.size))
              case vv =>
                // TODO: turn these into invalid argument type errors based on
                // the apply use site
                val chks = ASeq.newBuilder[TypeConstraintCheck]
                chks ++= uargs.view.zipWithIndex.map { case (uarg, i) =>
                  val vpar = vpars.lift(i).orElse(vv).get
                  constrain(uarg, ualt, vpar, valt)
                }
                chks += constrain(vret, valt, uret, ualt)
                suball(chks.result())
            }

          case (v: Type.Named, u @ Constraint.Apply(_, _, _))
              if typeShape(v.name).apply.isDefined =>
            val shape = typeShape(v.name)
            val sch = shape.apply.get
            val v0 = instantiateSchemeV(v, shape, sch)
            dbg(s"Resolving apply(): [$value]($sch) -> $v0")
            sub(constrain(v0, valt, u, ualt))

          // [] Access

          // delegate to field selection if possible
          case (
                Constraint.Rec(_, _, _),
                Constraint.Access(
                  ASeq(Constraint.Lit(Literal.Str(field), lspan)),
                  ret,
                  argsSpan)) =>
            val u0 = Constraint.Interface(field, ret, argsSpan, lspan)
            sub(constrain(value, valt, u0, ualt))

          // FIXME: we do not reject invalid indexes, only narrow to valid ones.
          // This is necessary because currently we unify Tup and Array to the union
          // of the two. In order to be able to opportunistically reject OOB access
          // when
          // possible, we need to figure out an alternative unification strategy
          // through
          // vars.
          case (
                Constraint.Tup(vs, _),
                Constraint.Access(
                  ASeq(Constraint.Lit(Literal.Int(idx), lspan)),
                  ret,
                  _)) =>
            // we can at least reject negative indexes
            if (idx < 0) {
              fail(
                TypeConstraintFailure
                  .InvalidTupleIndex(value, lspan, vs.length, idx.toInt))
            } else if (idx >= vs.length) {
              // OOB access errors, so type it to Never
              sub(constrain(Type.Never(value.span), valt, ret, ualt))
            } else {
              sub(constrain(vs(idx.toInt), valt, ret, ualt))
            }

          case (
                Constraint.Rec(v, vw, vspan),
                Constraint.Access(args, ret, argsSpan)) =>
            if (args.length == 1) {
              val chks = ASeq.newBuilder[TypeConstraintCheck]

              chks += constrain(args(0), ualt, Type.Str, valt)
              // Access could be selecting any of the fields, so `ret` must be a
              // subtype of all the fields in the record.
              chks ++= v.values.view.map(constrain(_, valt, ret, ualt))
              chks ++= vw.map(constrain(_, valt, ret, ualt))

              // Access may also return null
              chks += constrain(Type.Null(vspan), valt, ret, ualt)

              suball(chks.result())
            } else {
              fail(
                TypeConstraintFailure.InvalidArity(value, argsSpan, 1, args.length))
            }

          case (v: Type.Named, Constraint.Access(uargs, uret, uspan))
              if typeShape(v.name).access.isDefined =>
            val shape = typeShape(v.name)
            val sch = shape.access.get
            val v0 = instantiateSchemeV(v, shape, sch)
            dbg(s"Resolving access[]: [$value]($sch) -> $v0")
            val u0 = Constraint.Apply(uargs, uret, uspan)
            sub(constrain(v0, valt, u0, ualt))

          /** Projection. Delegates to the constrainProj helper, which breaks down the
            * match into further constrain calls.
            *
            * FIXME: this is essentially a lookup table of projection impls.
            * move to TypeShape?
            */

          case (Constraint.Rec(_, _, _), Constraint.Proj(p, u, _)) =>
            constrainProj(value, p, u, r => r)
          case (Type.Interface(_, _, _), Constraint.Proj(p, u, _)) =>
            constrainProj(value, p, u, r => r)

          // special-case Array, Set, and Stream named types
          case (Type.Array(v, span), Constraint.Proj(p, u, _)) =>
            constrainProj(v, p, u, Type.Array(_, span))
          case (Type.Set(v, span), Constraint.Proj(p, u, _)) =>
            constrainProj(v, p, u, Type.Set(_, span))
          case (Type.EventSource(v, span), Constraint.Proj(p, u, _)) =>
            constrainProj(v, p, u, Type.EventSource(_, span))

          case (Type.Null, Constraint.Proj(p, u, _)) =>
            val v = freshVar(value.span)
            val chk = constrainProj(v, p, u, _ => Type.Null)
            varUndo -= v
            chk

          // handle ref types
          case (
                v0 @ (Type.Ref(_, _) | Type.NamedRef(_, _)),
                Constraint.Proj(p, u, _)) =>
            val v = v0.asInstanceOf[Type.Named].args.head
            constrainProj(v, p, u, r => Type.Optional(r))

          // special-case empty-ref handling. This still constrains the
          // projection against the doc type, vs falling back to Null which ends
          // up allowing anything.
          case (
                v0 @ (Type.EmptyRef(_, _) | Type.EmptyNamedRef(_, _)),
                Constraint.Proj(p, u, _)) =>
            val v = v0.asInstanceOf[Type.Named].args.head
            constrainProj(v, p, u, _ => Type.Null)

          // Handler for legacy NullDoc aliases
          case (v: Type.Named, Constraint.Proj(p, u, _))
              if typeShape(v.name).alias == Some(Type.Null.typescheme) =>
            val v = freshVar(value.span)
            val chk = constrainProj(v, p, u, _ => Type.Null)
            varUndo -= v
            chk

          case (v: Type.Named, Constraint.Proj(p, u, _)) =>
            constrainProj(v, p, u, r => r)

          // Any punches a hole through static checks, but we still need to propagate
          // it.

          case (Type.Any(span), Constraint.Apply(_, r1, _)) =>
            // params are left unconstrained, but propagate any through the return
            constrain(Type.Any(span), valt, r1, ualt)
          case (Type.Any(span), Constraint.Access(_, r1, _)) =>
            // params are left unconstrained, but propagate any through the return
            constrain(Type.Any(span), valt, r1, ualt)
          case (Type.Any(span), Constraint.Interface(_, u, _, _)) =>
            // propagate any through field value
            constrain(Type.Any(span), valt, u, ualt)
          case (Type.Any(span), u: Type.Named) =>
            // propagate any through generic args
            // FIXME: look up arg co/contra-variance and do the right thing here
            val chks = u.args.view
              .map { case u => constrain(Type.Any(span), valt, u, ualt) }
            suball(chks)

          case (Type.Any(_), _) | (_, Type.Any(_)) =>
            ok()

          // Top is the supertype of everything
          case (_, Type.Top(_)) =>
            ok()

          // Never/bottom is a subtype of everything
          case (Type.Never(_), _) =>
            ok()

          /** Specialized checks have failed. Next few cases widen types. */

          // Widen tuple to Array. Only catch success cases here, so that error
          // cases below still get the tuple type.
          case (
                Constraint.Tup(vs, span),
                Constraint.Proj(_, _, _) | Constraint.Interface(_, _, _, _) |
                Constraint.Access(_, _, _) | Type.Array(_, _)) =>
            val v = freshVar(span)
            val chks = ASeq.newBuilder[TypeConstraintCheck]

            chks ++= vs.view.map(constrain(_, valt, v, ualt))
            chks += constrain(Type.Array(v, span), valt, use, ualt)
            varUndo -= v
            suball(chks.result())

          // Alias resolution

          case (v: Type.Named, _) if typeShape(v.name).alias.isDefined =>
            val shape = typeShape(v.name)
            val sch = shape.alias.get
            val v0 = instantiateSchemeV(v, shape, sch)
            dbg(s"Resolving alias: [$value]($sch) -> $v0")
            valias(constrain(v0, valt, use, ualt))

          case (_, u: Type.Named) if typeShape(u.name).alias.isDefined =>
            val shape = typeShape(u.name)
            val sch = shape.alias.get
            val u0 = instantiateSchemeU(u, shape, sch)
            dbg(s"Resolving alias (use): [$use]($sch) -> $u0")
            // NOTE: if we need a UAlias TypeConstraintCheck variant, this
            // is where we would use it. sub() seems to work.
            sub(constrain(value, valt, u0, ualt))

          /** Error cases. Concrete failures which stop constrain. */

          // Specialized error cases

          case (_, Constraint.Apply(_, _, argsSpan)) =>
            fail(
              TypeConstraintFailure.NotAFunction(
                value,
                // I tried making the value span non-null, but that'd require
                // .move()ing the bounds of a var, and this ended up just being
                // simpler.
                value.span.getOrElse(callSite),
                argsSpan
              ))

          case (_, Constraint.Access(_, _, argsSpan)) =>
            fail(TypeConstraintFailure.CannotAccess(value, argsSpan))

          case (_, Constraint.Interface(field, _, dotSpan, ispan)) =>
            fail(TypeConstraintFailure.MissingField(value, dotSpan, ispan, field))

          case (_, Constraint.Proj(_, _, _)) =>
            fail(TypeConstraintFailure.CannotProject(value, value.span))

          // Generic error case

          case (_: Type.Top, _) | (_: Type.Named, _) | (_: Type.Skolem, _) |
              (_: Constraint.Lit, _) | (_: Type.Singleton, _) |
              (_: Constraint.Func, _) | (_: Type.Function, _) |
              (_: Constraint.Rec, _) | (_: Type.Record, _) | (_: Type.Interface, _) |
              (_: Constraint.Tup, _) | (_: Type.Tuple, _) |
              (_: Constraint.Intersect, _) | (_: Type.Intersect, _) |
              (_: Constraint.Diff, _) =>
            val span = value.span.getOrElse(callSite)
            fail(TypeConstraintFailure.NotSubtype(value, use, span))
        }
      }
  }
}
