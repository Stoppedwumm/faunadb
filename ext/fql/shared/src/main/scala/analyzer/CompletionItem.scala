package fql.analyzer

import fql.ast.{ Span, TypeExpr }
import fql.ast.display._
import fql.typer.{ Constraint, Type, TypeScheme, Typer }

/** The client will replace everything in `span` with `replaceText`. For example,
  * to only insert text, set the `span` to a zero-length span at the cursor.
  */
case class CompletionItem(
  label: String,
  detail: String,
  span: Span,
  replaceText: String,
  newCursor: Int,
  kind: CompletionKind,
  retrigger: Boolean,
  snippet: Option[String] = None)

case class PartialCompletionItem(
  label: String,
  v: Constraint.Value,
  te: TypeExpr,
  kind: CompletionKind,
  priorityOffset: Int = 0) {

  lazy val isFunction = PartialCompletionItem.isTypeExprFunction(te)

  lazy val replaceText = kind match {
    case CompletionKind.Property => label + ": "
    case _ if isFunction         => label + "()"
    case _                       => label
  }

  lazy val retrigger = kind match {
    case CompletionKind.Property => true
    case _ if isFunction         => true
    case _                       => false
  }

  lazy val detail = kind match {
    case CompletionKind.Keyword => "A builtin keyword"
    case _                      => te.display
  }

  def newCursor(identSpan: Span) = kind match {
    // The -1 puts the cursor inside the parens from replaceText.
    case _ if isFunction => identSpan.start + replaceText.length - 1
    case _               => identSpan.start + replaceText.length
  }

  def returnType(typer: Typer): Type = {
    val ret = PartialCompletionItem.returnTypeOfTypeExpr(te)
    val sch = TypeExpr.Scheme(Seq.empty, ret)
    typer.typeTSchemeUncheckedType(sch)
  }

  def priority(typer: Typer, ty: Type): Int = {
    val BasePriority = 0
    val MatchingTypePriority = 10

    val VariablePriority = 2
    val ModulePriority = 1
    val KeywordPriorty = -1

    val base = kind match {
      case CompletionKind.Variable => VariablePriority
      case CompletionKind.Module   => ModulePriority
      case CompletionKind.Keyword  => KeywordPriorty
      case _                       => BasePriority
    }

    val priority = te match {
      // keywords are special, and completing `Any` is common, so these shouldn't
      // get prioritized unless we have a more specific type.
      case TypeExpr.Any(_) if kind == CompletionKind.Keyword => base
      case _ if typer.isSubtype(v, ty) => base + MatchingTypePriority
      case _ if isFunction && typer.isSubtype(returnType(typer), ty) =>
        base + MatchingTypePriority
      case _ => base
    }

    priority + this.priorityOffset
  }

  // builds a snippet item, like ${0:foo}
  //
  // The thing before the colon is an arbitrary key, used for ordering when
  // selecting. The thing on the right is the text to insert into the editor. For
  // example, a for loop might build snippets like so:
  //
  // ```
  // for (int ${0:i} = ${1}; ${0:i}; ${0:i}++) {
  //   ${2}
  // }
  // ```
  //
  // The above example will prompt the user to replace the first item (which will be
  // an "i", and it will be selected) with a variable name. Upon pressing tab, the
  // cursor will be moved to the `${1}`, etc.
  class SnippetBuilder {
    val sb = new StringBuilder
    var index = 0

    def add(ty: TypeExpr, name: String)(implicit indent: Int = 0): Unit = ty match {
      case TypeExpr.Lambda(args, _, ret, _) =>
        if (args.length != 1) {
          sb += '('
        }
        args.zipWithIndex.foreach { case ((name, ty), i) =>
          if (i != 0) {
            sb ++= ", "
          }
          add(ty, name.map(_.str).getOrElse(s"arg$i"))
        }
        if (args.length != 1) {
          sb += ')'
        }
        sb ++= " => "

        add(ret, "value")

      case _ =>
        index += 1
        sb ++= "${"
        sb ++= index.toString()
        sb += ':'
        sb ++= name
        sb += '}'
    }

    def +=(v: Char) = this.sb += v
    def ++=(v: String) = this.sb ++= v
    def result() = this.sb.result()
  }

  def allowMissingField(use: TypeExpr): Boolean = use match {
    case TypeExpr.Id(str, _) if str == "Null" => true
    case TypeExpr.Union(us, _)                => us.exists(allowMissingField(_))
    case _                                    => false
  }

  def snippet: Option[String] =
    if (isFunction) {
      val b = new SnippetBuilder
      b ++= label
      b += '('
      te match {
        case TypeExpr.Lambda(args, _, _, _) =>
          args.zipWithIndex.map { case ((name, ty), index) =>
            if (index != 0) {
              b ++= ", "
            }
            b.add(ty, name.map(_.str).getOrElse(s"arg$index"))
          }
        // FIXME: What to do about overloads?
        case _ => Seq.empty
      }
      b ++= s")$${0}"
      Some(b.result())
    } else {
      None
    }
}

object PartialCompletionItem {
  def apply(label: String, kind: CompletionKind) =
    new PartialCompletionItem(label, Type.Any, TypeExpr.Any(Span.Null), kind)

  def apply(typer: Typer, label: String, ts: TypeScheme, kind: CompletionKind) = {
    new PartialCompletionItem(label, ts.raw, typer.toTypeSchemeExpr(ts).expr, kind)
  }

  def apply(typer: Typer, label: String, v: Constraint.Value, kind: CompletionKind) =
    new PartialCompletionItem(label, v, typer.valueToExpr(v), kind)

  def apply(
    typer: Typer,
    label: String,
    v: Constraint.Value,
    kind: CompletionKind,
    priorityOffset: Int) =
    new PartialCompletionItem(label, v, typer.valueToExpr(v), kind, priorityOffset)

  def isTypeExprFunction(te: TypeExpr): Boolean = te match {
    case TypeExpr.Lambda(_, _, _, _) => true
    case TypeExpr.Intersect(tes, _)  => tes.forall(isTypeExprFunction(_))
    case _                           => false
  }

  def returnTypeOfTypeExpr(te: TypeExpr): TypeExpr = te match {
    case TypeExpr.Lambda(_, _, ret, _) => ret
    case TypeExpr.Intersect(tes, _) =>
      TypeExpr.Union(tes.map(returnTypeOfTypeExpr(_)), Span.Null)
    case _ => te
  }

  def apply(
    typer: Typer,
    label: String,
    v: Constraint.Value,
    kind: TypeExpr => CompletionKind) = {
    val te = typer.valueToExpr(v)
    new PartialCompletionItem(label, v, te, kind(te))
  }
}
