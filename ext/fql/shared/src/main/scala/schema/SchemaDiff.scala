package fql.schema

import fql.ast
import fql.ast._
import fql.ast.display._
import fql.migration.SchemaMigration
import scala.collection.immutable.ListSet
import scala.collection.mutable.SeqMap
import scala.language.implicitConversions

sealed trait Diff extends Ordered[Diff] {
  def item: SchemaItem
  final def span = item.span

  final def compare(that: Diff): Int = this.category.compare(that.category) match {
    case 0 =>
      item.kind.compare(that.item.kind) match {
        case 0 => item.name.str.compare(that.item.name.str)
        case n => n
      }
    case n => n
  }

  private def category: Int = {
    this match {
      case _: Diff.Add    => 0
      case _: Diff.Remove => 1
      case _: Diff.Modify => 2
    }
  }
}

object Diff {
  final case class Add(item: SchemaItem) extends Diff
  final case class Remove(item: SchemaItem) extends Diff

  final case class Modify(
    before: SchemaItem,
    after: SchemaItem,
    changes: Seq[Change],
    migrations: Seq[SchemaMigration] = Seq.empty)
      extends Diff {
    require(changes.nonEmpty, "modification requires changes")
    def isMove = before.span.src != after.span.src
    def item = before
  }
}

sealed trait Change {

  /** The type of change being made. `None` means the change should not be
    * rendered.
    */
  def category: Option[Category]
}

sealed trait Category {
  def name: String
}

object Category {
  case object ComputedField extends Category { def name = "Computed fields" }
  case object Index extends Category { def name = "Indexes" }
  case object Constraint extends Category { def name = "Constraints" }
  case object Configuration extends Category { def name = "Configuration" }

  def all = Seq(ComputedField, Index, Constraint, Configuration)

  def forMember(mem: ast.Member): Option[Category] = mem.kind match {
    case ast.Member.Kind.Index      => Some(Category.Index)
    case ast.Member.Kind.Unique     => Some(Category.Constraint)
    case ast.Member.Kind.Check      => Some(Category.Constraint)
    case ast.Member.Kind.Migrations => None
    case _                          => Some(Category.Configuration)
  }
}

object Change {
  import SchemaItem._

  sealed trait Element {
    def display: String
    def configSpan: Span
    def span: Span
    def category: Option[Category]
  }

  object Element {
    final case class Annotation(ann: ast.Annotation) extends Element {
      def display = ann.display
      def configSpan = ann.config.span
      def span = ann.span

      def category: Option[Category] = Some(Category.Configuration)
    }

    final case class Field(f: ast.Field) extends Element {
      def display = f.display
      def configSpan = f.value match {
        case Some(v) => v.span
        case None    => Span.Null
      }
      def span = f.span

      def category = f.kind match {
        case FSL.FieldKind.Compute => Some(Category.ComputedField)
        case _                     => None
      }
    }

    final case class Member(mem: ast.Member) extends Element {
      def display = mem.display
      def configSpan = mem.config.span
      def span = mem.span

      def category = Category.forMember(mem)
    }

    implicit def apply(ann: ast.Annotation) = new Annotation(ann)
    implicit def apply(f: ast.Field) = new Field(f)
    implicit def apply(mem: ast.Member) = new Member(mem)
  }

  final case class Rename(from: Name, to: Name) extends Change {
    def category = Some(Category.Configuration)
  }
  final case class Add(element: Element) extends Change {
    def category = element.category
  }
  final case class Remove(element: Element) extends Change {
    def category = element.category
  }
  final case class Modify(element: Element, config: Config) extends Change {
    def category = element.category
  }
  final case class Block(value: Member, changes: Seq[Change]) extends Change {
    def category = Category.forMember(value)
  }
  final case class ReplaceBody(before: Expr.Block, after: Expr.Block)
      extends Change {
    def category = None
  }
  final case class ReplaceSig(before: Function.Sig, after: Function.Sig)
      extends Change {
    def category = None
  }
  final case class ReplaceType(before: Field, after: TypeExpr) extends Change {
    def category = None
  }
  final case class ReplaceSchemaType(before: Field, after: SchemaTypeExpr)
      extends Change {
    def category = None
  }
  final case class RemoveFieldDefault(field: Field) extends Change {
    require(field.value.isDefined, "field has no default")
    def category = Some(Category.Configuration)
  }
}

object SchemaDiff {
  import SchemaItem._

  type Renames = Map[(SchemaItem.Kind, String), String]

  def diffItems(
    before: Seq[SchemaItem],
    after: Seq[SchemaItem],
    renames: Renames): Seq[Diff] = {

    lazy val oldIdByName =
      before.groupMapReduce(item => (item.kind, item.name.str))(_.id) {
        case (_, id) => throw new IllegalArgumentException(s"duplicated item $id")
      }

    def renamedId(item: SchemaItem) = {
      renames
        .get((item.kind, item.name.str))
        .flatMap { oldName =>
          oldIdByName.get((item.kind, oldName))
        }
    }

    alignedDiff(before, after)(
      Diff.Add(_),
      Diff.Remove(_),
      diffItem(_, _),
      renamedId(_)
    )
  }

  def diffItem(before: SchemaItem, after: SchemaItem): Option[Diff.Modify] =
    (before, after) match {
      case (b: ItemConfig, a: ItemConfig) => diffItemConfig(b, a)
      case (b: Function, a: Function)     => diffFunction(b, a)
      case _                              => sys.error("unreachable")
    }

  private def addImplicitWildcard(fields: Seq[Field]): Seq[Field] = {
    var noWC = true
    var noDF = true
    fields foreach {
      case _: Field.Wildcard => noWC = false
      case _: Field.Defined  => noDF = false
      case _                 => // Pass.
    }
    if (noWC && noDF) {
      fields :+ Field.Wildcard(TypeExpr.Any(Span.Null), Span.Null)
    } else {
      fields
    }
  }

  private def diffItemConfig(
    before: ItemConfig,
    after: ItemConfig): Option[Diff.Modify] = {
    val names = diffNames(before.name, after.name).toSeq
    val annotations = diffAnnotations(before.annotations, after.annotations)
    val fields = diffFields(
      addImplicitWildcard(before.fields),
      addImplicitWildcard(after.fields))
    val members = diffMembers(before.members, after.members)
    val changes = names ++ annotations ++ fields ++ members
    Option.when(changes.nonEmpty) { Diff.Modify(before, after, changes) }
  }

  private def diffNames(before: Name, after: Name): Option[Change] =
    Option.when(before.str != after.str) {
      Change.Rename(before, after)
    }

  private def diffAnnotations(
    before: Seq[Annotation],
    after: Seq[Annotation]
  ): Seq[Change] =
    alignedDiff(before, after)(
      Change.Add(_),
      Change.Remove(_),
      diffAnnotation(_, _)
    )

  private def diffAnnotation(before: Annotation, after: Annotation): Option[Change] =
    diffScalar(before.config, after.config) map {
      Change.Modify(before, _)
    }

  private def diffFields(before: Seq[Field], after: Seq[Field]): Seq[Change] =
    alignedDiff(before, after)(
      e => Seq(Change.Add(e)),
      e => Seq(Change.Remove(e)),
      diffField(_, _)
    ).flatten

  private def diffField(before: Field, after: Field): Option[Seq[Change]] = {
    val b = Seq.newBuilder[Change]
    (before, after) match {
      case (Field.Wildcard(bf, _), Field.Wildcard(af, _)) =>
        b ++= Option.when(af.withNullSpan != bf.withNullSpan)(
          Change.ReplaceType(before, af))
      case _ =>
        (before.value, after.value) match {
          case (None, None)     => ()
          case (Some(_), None)  => b += Change.RemoveFieldDefault(before)
          case (None, Some(av)) => b += Change.Modify(before, av)
          case (Some(bv), Some(av)) =>
            diffScalar(bv, av).foreach { scalar =>
              b += Change.Modify(before, scalar)
            }
        }

        (before, after) match {
          case (Field.Defined(_, bte, _, _), Field.Defined(_, ate, _, _)) =>
            if (bte.withNullSpan != ate.withNullSpan) {
              b += Change.ReplaceSchemaType(before, ate)
            }

          case _ =>
            // TODO: This always applies to changing the signature
            // of a defined field, but it will miss adding or removing a signature
            // from a computed field.
            if (before.ty.isDefined && after.ty.isDefined) {
              val aty = after.ty.get
              b ++= Option.when(before.ty.get.withNullSpan != aty.withNullSpan)(
                Change.ReplaceType(before, aty))
            }
        }

    }
    val cs = b.result()
    Option.when(cs.nonEmpty)(cs)
  }

  private def diffMembers(before: Seq[Member], after: Seq[Member]): Seq[Change] =
    alignedDiff(before, after)(
      Change.Add(_),
      Change.Remove(_),
      diffMember(_, _)
    )

  private def diffMember(before: Member, after: Member): Option[Change] = {
    def diffConfig(beforeConfig: Config, afterConfig: Config): Option[Change] =
      (beforeConfig, afterConfig) match {
        case (b: Config.Scalar, a: Config.Scalar) =>
          diffScalar(b, a) map {
            Change.Modify(before, _)
          }

        case (b: Config.Seq, a: Config.Seq) =>
          diffSeq(b, a) map {
            Change.Modify(before, _)
          }

        case (b: Config.Block, a: Config.Block) =>
          val changes = diffMembers(b.members, a.members)
          Option.when(changes.nonEmpty) {
            Change.Block(before, changes)
          }

        case (b: Config.Opt, a: Config.Opt) =>
          (b.config, a.config) match {
            case (None, None) => None
            case (Some(bC), Some(aC)) =>
              Option.when(diffConfig(bC, aC).nonEmpty) {
                Change.Modify(before, afterConfig)
              }
            case _ =>
              Some(Change.Modify(before, afterConfig))
          }

        case (_, a) =>
          Some(Change.Modify(before, a)) // different types
      }

    (before, after) match {
      // The change, rather implicit or not, makes no difference.
      case (_: Member.Default[_], _: Member.Default[_])
          if before.isDefault && after.isDefault =>
        None

      // Adding a config that changes a default.
      case (_: Member.Default.Implicit[_], _: Member.Default.Set[_])
          if before.isDefault =>
        Some(Change.Add(after))

      // Removing a config that resets a default.
      case (_: Member.Default.Set[_], _: Member.Default.Implicit[_])
          if after.isDefault =>
        Some(Change.Remove(before))

      // Other config chnages, including two implicit members not present in the FSL,
      // like a default config that may have changed in recent versions.
      case _ =>
        diffConfig(before.config, after.config)
    }
  }

  private def diffScalar(
    before: Config.Scalar,
    after: Config.Scalar
  ): Option[Config.Scalar] =
    Option.when(before.id != after.id) { after }

  private def diffSeq(before: Config.Seq, after: Config.Seq): Option[Config.Seq] = {
    val b = before.configs
    val a = after.configs
    val changed =
      b.sizeIs != a.size || b.zip(a).exists { case (b, a) =>
        diffScalar(b, a).nonEmpty
      }
    Option.when(changed) { after }
  }

  private def diffFunction(
    before: Function,
    after: Function): Option[Diff.Modify] = {
    val changes = Seq.newBuilder[Change]
    changes ++= diffNames(before.name, after.name)
    changes ++= diffAnnotations(before.annotations, after.annotations)

    def sigId(fn: Function) =
      fn.sig.args.view
        .map { arg => (arg.name.str, arg.ty map { _.withNullSpan }, arg.variadic) }
        .appended { fn.sig.ret map { _.withNullSpan } }
        .toSeq

    if (sigId(before) != sigId(after)) {
      changes += Change.ReplaceSig(before.sig, after.sig)
    }

    if (before.body.withNullSpan != after.body.withNullSpan) {
      changes += Change.ReplaceBody(before.body, after.body)
    }

    val changes0 = changes.result()
    Option.when(changes0.nonEmpty) {
      Diff.Modify(before, after, changes0)
    }
  }

  private def alignedDiff[A <: GroupedItem, R](before: Seq[A], after: Seq[A])(
    add: A => R,
    rm: A => R,
    diff: (A, A) => Option[R],
    renamedId: A => Option[AnyRef] = scala.Function.const(None) _
  ): Seq[R] = {
    def groupBy(items: Seq[A])(fn: A => AnyRef) = {
      val b = SeqMap.empty[AnyRef, Seq[A]].withDefaultValue(Seq.empty)

      items.foreach { item =>
        val key = fn(item)
        b(key) = b(key) :+ item
      }

      b
    }

    // group by id to preserve duplicated items
    val beforeById = groupBy(before) { _.id }
    val afterById = groupBy(after) { a => renamedId(a).getOrElse(a.id) }

    // consider the number of duplicated items to detect if we added or removed items
    val beforeKs = beforeById.map { case (k, v) => (k, v.size) }.to(ListSet)
    val afterKs = afterById.map { case (k, v) => (k, v.size) }.to(ListSet)
    val b = Seq.newBuilder[R]

    (afterKs -- beforeKs) foreach { case (id, _) =>
      afterById(id) foreach { item =>
        if (!item.isDefault) b += add(item)
      }
    }
    (beforeKs -- afterKs) foreach { case (id, _) =>
      beforeById(id) foreach { item =>
        if (!item.isDefault) b += rm(item)
      }
    }
    (beforeKs & afterKs) foreach { case (id, _) =>
      beforeById(id).zip(afterById(id)) foreach { case (before, after) =>
        b ++= diff(before, after)
      }

    }

    b.result()
  }
}
