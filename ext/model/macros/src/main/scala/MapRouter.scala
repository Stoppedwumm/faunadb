package fauna.ast

import fauna.atoms.APIVersion
import scala.annotation.{ compileTimeOnly, tailrec }
import scala.collection.mutable.{ Map => MMap, Queue => MQueue, Set => MSet }
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

/**
  * The MapRouter builds a simple interpreter for an orderless
  * language.
  *
  * At compile time, for each symbol (a set of keys):
  *   - compute all possible subsets of the symbol ("patterns")
  *   - emit a table of pattern -> index pairs
  *   - emit a table of index -> handler pairs
  *
  * At runtime:
  *   - spill arguments (key -> value pairs) onto the "stack"
  *   - jump from input pattern -> index -> handler
  *
  * New symbols are added to the language using `form()` and `function()`.
  *
  * The interpreter is emitted with `build()`.
  */
object MapRouter {
  class Form[H](val keys: List[(String, Boolean)], val pred: Option[APIVersion => Boolean]) extends (H => Unit) { def apply(h: H) = () }

  class Builder[I, R, C] {
    @compileTimeOnly("Calls to `add` must be inside `build` macro.")
    def add(defs: Any*): Nothing => Any = macro MapRouterImpl.form[I, R, C]

    @compileTimeOnly("Calls to `addFunction` must be inside `build` macro.")
    def addFunction(defs: Any*): Any = macro MapRouterImpl.function
  }

  def build[I, R, C](f: Builder[I, R, C] => Unit): MapRouter[I, R, C] = macro MapRouterImpl.build[I, R, C]
}

abstract class MapRouter[I, R, C] {
  def apply(argList: List[(String, I)], ctx: C, apiVersion: APIVersion): Option[R]
}

class MapRouterImpl(val c: Context) {
  import c.universe._

  @annotation.nowarn("cat=unused-pat-vars")
  def form[I: c.WeakTypeTag, R: c.WeakTypeTag, C: c.WeakTypeTag](defs: c.Tree*) = {
    val I = implicitly[WeakTypeTag[I]]
    val R = implicitly[WeakTypeTag[R]]
    val C = implicitly[WeakTypeTag[C]]

    val (pred, kDefs) = defs.last match {
      case q"(_: $arg) => $body" =>
        val pred = defs.last

        (Some(pred), defs.init)

      case pred @ Apply(_, _) =>
        (Some(pred), defs.init)

      case _ =>
        (None, defs)
    }

    val fieldConfig = kDefs map { k => q"($k, true)" } toList

    val H = {
      val is = kDefs map { _ => tq"$I" }

      tq"(..$is, $C, _root_.fauna.atoms.APIVersion) => $R"
    }

    q"""new _root_.fauna.ast.MapRouter.Form[$H]($fieldConfig, $pred)"""
  }

  @annotation.nowarn("cat=unused-pat-vars")
  def function(defs: c.Tree*) = {
    val I = tq"_root_.fauna.ast.Literal"
    val R = tq"_root_.fauna.repo.query.Query[_root_.fauna.ast.PResult[_root_.fauna.ast.Expression]]"
    val C = tq"(_root_.fauna.auth.Auth, _root_.fauna.ast.Position)"

    val (pred, fn, nDefs) = defs.last match {
      case q"(_: $arg) => $body" =>
        val pred = defs.last
        val nDefs = defs.init

        (Some(pred), nDefs.last, nDefs.init)

      case pred @ Apply(_, _) =>
        val nDefs = defs.init

        (Some(pred), nDefs.last, nDefs.init)

      case _ =>
        (None, defs.last, defs.init)
    }

    val params = nDefs.toList map {
      case q"($_[$_](${keyDef: String})).->[$_]($castDef)" =>

        val (cast, isReq) = castDef match {
          case q"scala.Option.apply[$targs]($c)" =>
            (c, false)

          case c =>
            (c, true)
        }

        (keyDef, isReq, cast)
      case d => throw new IllegalStateException(s"Unknown def $d")
    }

    val H = {
      val is = params map {
        case (_, true, _)  => tq"$I"
        case (_, false, _) => tq"_root_.scala.Option[$I]"
      }

      tq"(..$is, $C, _root_.fauna.atoms.APIVersion) => $R"
    }

    val args = (1 to params.size) map { i => TermName(s"a$i") }
    val argPats = args map { a => pq"$a @ _" }

    val fieldConfig = params map { case (k, r, _) => q"($k, $r)" }

    val parses = params zip args map {
      case ((k, true, _), a)  => q"parseExpr(auth, $a, apiVersion, pos at $k)"
      case ((k, false, _), a) => q"parseOpt(auth, $a, apiVersion, pos at $k)"
    }

    val maxEffect = (params zip args map {
      case ((_, true, _), a) => q"$a.maxEffect"
      case ((_, false, _), a) => q"$a.fold(_root_.fauna.model.runtime.Effect.Pure: _root_.fauna.model.runtime.Effect)(_.maxEffect)"
    } foldLeft (q"$fn.effect": c.Tree)) { (a, b) => q"$a + $b" }


    val evals = params zip args map {
      case ((k, true, c), a) => q"ec.eval($a) map { _.flatMap { $c(_, pos at $k) } }"
      case ((k, false, c), a) => q"ec.evalOpt($a) map { _.flatMap { $c.opt(_, pos at $k) } }"
    }

    val lits = params zip args map {
      case ((k, true, _), a) => q"Some($k -> $a.literal)"
      case ((k, false, _), a) => q"$a map { a => $k -> a.literal }"
    }

    val tracingOpName = s"eval.native_call.${params.head._1}"

    // FIXME: Optimization possibilities:
    // 1. bind casts outside of eval application in order to not
    //    create them all the time.
    // 2. ???

    if (params.size == 1) {
      q"""(new _root_.fauna.ast.MapRouter.Form[$H]($fieldConfig, $pred)).apply {
        case (${argPats.head}, (auth, pos), apiVersion) =>
          ${parses.head} mapT { case (${args.head}) =>
            _root_.fauna.ast.NativeCall({ ec: _root_.fauna.ast.EvalContext =>
              _root_.fauna.trace.GlobalTracer.instance.activeSpan foreach {
                _.setOperation($tracingOpName)
              }
              ${evals.head} flatMapT { $fn(_, ec, pos) }
            }, () => ObjectL($lits.flatten: _*), $maxEffect, $fn.effect, pos)
          }
      }"""
    } else {
      q"""(new _root_.fauna.ast.MapRouter.Form[$H]($fieldConfig, $pred)).apply {
        case(..$argPats, (auth, pos), apiVersion) =>
          (..$parses) parT {
            case (..$argPats) =>
              _root_.fauna.ast.PResult.successfulQ(
                _root_.fauna.ast.NativeCall({ ec: _root_.fauna.ast.EvalContext =>
                  _root_.fauna.trace.GlobalTracer.instance.activeSpan foreach {
                    _.setOperation($tracingOpName)
                  }
                  (..$evals) parT {
                    case (..$argPats) => $fn(..$args, ec, pos)
                  }
                }, () => ObjectL($lits.flatten: _*), $maxEffect, $fn.effect, pos))
          }
      }"""
    }
  }

  def build[I: c.WeakTypeTag, R: c.WeakTypeTag, C: c.WeakTypeTag](f: c.Tree): c.Tree = {
    val I = implicitly[WeakTypeTag[I]]
    val R = implicitly[WeakTypeTag[R]]
    val C = implicitly[WeakTypeTag[C]]

    val slotByKey = MMap.empty[String, (Int, MSet[String])]
    val versionsByKeys = MMap.empty[Set[String], List[Set[APIVersion]]]
    val idxByPattern = MMap.empty[Set[Set[String]], Int]

    var hID = 0

    def newIndex: Int = {
      val id = hID
      hID += 1
      id
    }

    def assignSlots(keys: List[String]): Unit = {
      val unassignedKeys = MSet.empty[String]
      val assignedSlots = MSet.empty[Int]

      @tailrec
      def assign(key: String): Unit = {
        slotByKey.get(key) match {
          case None =>
            unassignedKeys += key

          case Some((slot, relatedKeys)) =>
            if (assignedSlots.add(slot)) {
              // Associate these keys with the other keys sharing the same slot.
              relatedKeys ++= keys
            } else {
              // The slot found conflicts with another key's slot for the same
              // function. Find within all associated keys the maximum slot
              // allocated, then reassign this key to the next slot.
              //
              // For example:
              // keyA(slot0), keyB(slot1),
              // keyX(slot0), keyY(slot1),
              // keyJ(slot0), keyY(slot1), keyB(slot1) <= conflict
              //
              // Re-assign:
              // keyA(slot0),            , keyB(slot2)
              // keyX(slot0), keyY(slot1),
              // keyJ(slot0), keyY(slot1), keyB(slot2)
              val keysToSearch = MQueue.empty[String]
              val searchedKeys = MSet.empty[String]
              var maxSlot = 0

              keysToSearch ++= relatedKeys

              while (keysToSearch.nonEmpty) {
                val key = keysToSearch.dequeue()
                if (searchedKeys.add(key)) {
                  slotByKey.get(key) foreach {
                    case (assignedSlot, related) =>
                      maxSlot = Math.max(assignedSlot, maxSlot)
                      keysToSearch ++= related
                  }
                }
              }

              slotByKey(key) = (maxSlot + 1, relatedKeys)
              assign(key) // re-assign
            }
        }
      }

      for (key <- keys) assign(key)

      var nextSlot = 0
      for (key <- unassignedKeys) {
        while (assignedSlots contains nextSlot) nextSlot += 1
        slotByKey(key) = (nextSlot, MSet(keys: _*))
        assignedSlots += nextSlot
        nextSlot += 1
      }
    }

    val allApiVersions = APIVersion.Versions

    val forms = f.collect {
      case q"new fauna.ast.MapRouter.Form[$_]($ks, $pred).apply($h)" =>
        val name = TermName(c.freshName("handler$"))
        // create tuple to batch the call to c.untypecheck as it is very expensive
        val tuple = q"(${ks.duplicate}, ${pred.duplicate})"
        val (keys, predOpt) = c.eval(
          c.Expr[(List[(String, Boolean)], Option[APIVersion => Boolean])](
            c.untypecheck(tuple)))
        val required = keys.iterator.collect { case (k, true) => k }.toSet
        val optional = keys.iterator.collect { case (k, false) => k }.toSet
        val patterns = optional.subsets().map { _ ++ required }.toSet
        val validVersions = predOpt match {
          case Some(p) => allApiVersions filter { p(_) }
          case None    => allApiVersions
        }

        patterns foreach { sub =>
          if (sub.isEmpty) c.abort(ks.pos, s"Handler must have at least 1 required argument.")

          if (versionsByKeys contains sub) {
            val versions = versionsByKeys(sub)

            versions foreach { vs =>
              val v = validVersions intersect vs
              if (v.nonEmpty) c.abort(ks.pos, s"Duplicate handler for key set $sub and versions $v, check your predicates.")
            }

            versionsByKeys += (sub -> (versions :+ validVersions))
          } else {
            versionsByKeys += (sub -> List(validVersions))
          }
        }

        //if the patterns apply to all api versions then no version predicate was applied,
        //so an unique id will be generated
        val newPatterns = if (validVersions == allApiVersions) {
          Set(patterns)
        } else {
          //otherwise, different patterns might have a different api versions, in that case
          //a different id will be generated for each combination of arguments.
          patterns map { pat => Set(pat) }
        }

        newPatterns map { patterns =>
          val idx = if (idxByPattern.contains(patterns)) {
            idxByPattern(patterns)
          } else {
            assignSlots(keys map { case (k, _) => k })
            val idx = newIndex
            idxByPattern += (patterns -> idx)
            idx
          }

          (name, idx, keys, patterns, h, pred)
        }
    } flatten

    val bindings = idxByPattern.toList flatMap {
      case (pats, idx) =>
        pats map { pat =>
          q"bind(Set(..${pat.toList}), $idx)"
        }
    }

    val handlerVals0 = forms map {
      case (name, _, _, _, h, _) => (name, h)
    } toSet

    val handlerVals = handlerVals0 map {
      case (name, h) =>
        q"private val $name = $h"
    }

    val slotVars = {
      val slotIDs = slotByKey.toList map { case (_, (slotID, _)) => slotID }
      slotIDs.distinct.sorted map { id: Int =>
        q"var ${TermName(s"slot$id")}: $I = null"
      }
    }

    val slotAssignment = {
      val branches = slotByKey map {
        case (key, (slotID, _)) =>
          cq"$key => ${TermName(s"slot$slotID")} = arg; rest = rest.tail"
      }

      q"""(key) match {
        case ..$branches
        case _ =>
          rest = _root_.scala.Nil
      }"""
    }

    val handlerMatchDef = {
      val entries = forms map { case (name, idx, keys, _, _, pred) =>
        val args = keys map { case (key, required) =>
          val (slotID, _) = slotByKey(key)
          val slot = TermName(s"slot$slotID")
          if (required) q"$slot" else q"_root_.scala.Option($slot)"
        }

        val dispatch = q"$name(..$args, ctx, apiVersion)"

        idx -> (dispatch -> pred)
      } sortBy { _._1 }

      val branches = entries map {
        case (i, (d, q"scala.None")) =>
          cq"$i => $d"

        case (i, (d, q"scala.Some.apply[$_]($pred)")) =>
          cq"$i if $pred(apiVersion) => $d"
        case entry => throw new IllegalStateException(s"Unknown entry $entry")
      }

      q"""Option(hID match {
        case ..$branches
        case _ => null
      })"""
    }

    val keyT = tq"_root_.scala.collection.Set[_root_.java.lang.String]"

    val applyDef = {
      val argsListT = tq"_root_.scala.List[(_root_.java.lang.String, $I)]"
      q"""def apply(argList: $argsListT, ctx: $C, apiVersion: _root_.fauna.atoms.APIVersion): _root_.scala.Option[$R] = {
        ..$slotVars

        var rest = argList
        val keys = _root_.scala.collection.Set.newBuilder[_root_.java.lang.String]

        while (rest.nonEmpty) {
          val (key, arg) = rest.head
          keys += key
          $slotAssignment
        }

        symbolTable.get(keys.result()) flatMap { hID =>
          $handlerMatchDef
        }
      }"""
    }

    q"""new _root_.fauna.ast.MapRouter[$I, $R, $C] {
      private val symbolTable = {
        val b = Map.newBuilder[$keyT, _root_.scala.Int]

        def bind(pat: $keyT, name: _root_.scala.Int) =
          b += pat -> name

        ..$bindings

        b.result()
      }

      ..$handlerVals

      $applyDef
    }"""
  }

}
