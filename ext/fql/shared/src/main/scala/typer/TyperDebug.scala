package fql.typer

import scala.collection.mutable.{ Set => MSet, Stack }

object TyperDebug {
  def dbg(msg: => Any)(implicit ctx: TyperDebugCtx): Unit =
    if (ctx._debugIsEnabled) ctx._debugMessage(msg)

  def dbgFrame[T](msg: => Any)(f: => T)(implicit ctx: TyperDebugCtx): T =
    if (!ctx._debugIsEnabled) {
      f
    } else {
      ctx._debugFrame(msg)(f)
    }

  def dbgEnabled(implicit ctx: TyperDebugCtx) = ctx._debugIsEnabled

  def getBounds(ty: Constraint): Map[Type.Var, Type.Var.Bounds] =
    getVars(ty).iterator.flatMap(v => v.bounds.map((v, _))).toMap

  def getVars(ty: Constraint): Set[Type.Var] = {
    val vars = MSet.empty[Type.Var]
    val stack = Stack(ty)

    while (stack.nonEmpty) {
      stack.pop() match {
        case Constraint.Lazy(v) => stack.push(v)

        case _: Type.Skolem =>

        case v: Type.Var if !vars.add(v) =>
        case v: Type.Var                 =>
          // FIXME: properly filter based on alts
          stack.addAll(v.values.iterator.concat(v.uses).map(_._1))

        case t: Type.Named => stack.addAll(t.args)

        case _: Constraint.Unleveled =>

        case v: Constraint.Func =>
          stack.addAll(v.params.map(_._2))
          stack.addAll(v.variadic.map(_._2))
          stack.push(v.ret)
        case u: Constraint.Apply =>
          stack.addAll(u.args)
          stack.push(u.ret)
        case t: Type.Function =>
          stack.addAll(t.params.map(_._2))
          stack.addAll(t.variadic.map(_._2))
          stack.push(t.ret)

        case u: Constraint.Access =>
          stack.addAll(u.args)
          stack.push(u.ret)

        case v: Constraint.Rec => stack.addAll(v.fields.values)
        case t: Type.Record =>
          stack.addAll(t.fields.values)
          stack.addAll(t.wildcard)

        case u: Constraint.Interface => stack.push(u.ret)
        case t: Type.Interface       => stack.push(t.ret)

        case u: Constraint.Proj =>
          stack.push(u.proj)
          stack.push(u.ret)

        case v: Constraint.Tup => stack.addAll(v.elems)
        case t: Type.Tuple     => stack.addAll(t.elems)

        // FIXME: properly filter based on alts?
        case v: Constraint.Intersect => stack.addAll(v.variants.map(_._2))
        case t: Type.Intersect       => stack.addAll(t.variants)

        case u: Constraint.Union => stack.addAll(u.variants.map(_._2))
        case t: Type.Union       => stack.addAll(t.variants)

        case v: Constraint.Diff =>
          stack.push(v.value)
          stack.push(v.sub)
        case u: Constraint.DiffGuard =>
          stack.push(u.sub)
          stack.push(u.use)
      }
    }

    vars.toSet
  }
}

trait TyperDebugCtx {
  def _debugIsEnabled: Boolean
  def _debugMessage(msg: => Any): Unit
  def _debugFrame[T](msg: => Any)(f: => T): T
}

trait TyperDebug extends TyperDebugCtx { self: Typer =>

  private[this] var dbgPrefix = ""
  protected implicit def _debugCtx: TyperDebugCtx = this

  protected def dbg(msg: => Any) = TyperDebug.dbg(msg)
  protected def dbgFrame[T](msg: => Any)(f: => T) = TyperDebug.dbgFrame(msg)(f)
  protected def dbgEnabled = TyperDebug.dbgEnabled

  def _debugIsEnabled = Typer.debugLogging

  def _debugMessage(msg: => Any): Unit =
    if (_debugIsEnabled) {
      val lines = s"$msg".split('\n')
      if (lines.isEmpty) {
        println(dbgPrefix)
      } else {
        lines.foreach(l => println(s"$dbgPrefix$l"))
      }
    }

  def _debugFrame[T](msg: => Any)(f: => T): T =
    if (!_debugIsEnabled) {
      f
    } else {
      _debugMessage(s"╭ $msg")
      val prev = dbgPrefix
      dbgPrefix = s"${dbgPrefix}│ "
      try {
        f
      } finally {
        dbgPrefix = prev
        _debugMessage("╰")
      }
    }
}
