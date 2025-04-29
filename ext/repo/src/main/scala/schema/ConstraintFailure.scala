package fauna.repo.schema

import fauna.atoms._
import fauna.repo.schema.Path
import fql.ast.display._
import fql.typer.Type

/** Variants for runtime constraint failures.
  */
sealed trait ConstraintFailure {
  def label: String = fields.map(_.toString).mkString(",")
  def fields: List[Path]

  // For V1 FQL2 the only real relevant constraint failure here is ReservedField.
  // TODO: Update the other constraint failure messages once we add schema support.
  // We will want to align value types because our other value types have the
  // displayString method
  import ConstraintFailure._
  def message = this match {
    case _: ImmutableFieldUpdate => "Failed to update field because it is immutable"
    case _: ReadOnlyFieldUpdate  => "Failed to update field because it is readonly"
    case _: ComputedFieldOverwrite => "Cannot write to a computed field"
    case e: TypeMismatch =>
      s"Expected ${e.expected.toFQL.display}, provided ${e.provided.toFQL.display}"

    // FIXME: Print what other collection! Need to move this error message
    // generation to a Query to lookup that collection though.
    case e: CollectionMismatch =>
      s"Expected ${e.expected.display} document, provided document from another collection"

    case _: InvalidField  => "Unexpected field provided"
    case _: ReservedField => "Is a reserved field and cannot be used"
    case e: MissingField =>
      s"Missing required field of type ${e.expected.toFQL.display}"
    case _: InvalidTupleArity        => "Invalid tuple arity"
    case cf: ValidatorFailure        => cf.msg
    case _: UniqueConstraintFailure  => "Failed unique constraint"
    case ccf: CheckConstraintFailure => ccf.msg
    case f: DefaultValueFailure      => s"Default field evaluation failed.\n${f.msg}"
    case _: FQL4UniqueConstraintFailure => "Failed unique constraint"
    case TenantRootWriteFailure(collName) =>
      s"A $collName cannot be created in your account root"
    case StagedDeletionFailure(collName) =>
      s"Cannot delete a $collName when schema is staged"
    case StagedRenameFailure(collName) =>
      s"Cannot rename a $collName when schema is staged"
    case umf: UnionMissingFields   => umf.msg
    case pmf: ProtectedModeFailure => pmf.msg
  }
}

object ConstraintFailure {

  sealed trait FieldConstraintFailure extends ConstraintFailure {
    def path: Path
    def fields = List(path)
  }

  // When an immutable value is updated.
  final case class ImmutableFieldUpdate(
    path: Path,
    expected: SchemaType,
    field: String)
      extends FieldConstraintFailure

  // When an update is attempted on a read-only field.
  final case class ReadOnlyFieldUpdate(
    path: Path,
    expected: SchemaType,
    field: String)
      extends FieldConstraintFailure

  // When writing in a computed field is attempted.
  final case class ComputedFieldOverwrite(field: String)
      extends FieldConstraintFailure {
    def path = Path(Right(field))
  }

  // When a value doesn't conform to an expected type.
  final case class TypeMismatch(
    path: Path,
    expected: SchemaType,
    provided: SchemaType)
      extends FieldConstraintFailure

  final case class CollectionMismatch(
    path: Path,
    expected: Type,
    provided: CollectionID)
      extends FieldConstraintFailure

  // When an object value has an unexpected field.
  final case class InvalidField(
    path: Path,
    expected: SchemaType,
    provided: SchemaType,
    field: String)
      extends FieldConstraintFailure

  // When a top-level document field is a reserved field.
  final case class ReservedField(path: Path, provided: SchemaType, field: String)
      extends FieldConstraintFailure

  // When an object is missing a required field (equivalent to null type mismatch).
  final case class MissingField(path: Path, expected: SchemaType, field: String)
      extends FieldConstraintFailure

  // When a tuple has the wrong number of elements.
  final case class InvalidTupleArity(
    path: Path,
    expected: SchemaType.Tuple,
    provided: SchemaType.Tuple)
      extends FieldConstraintFailure

  // When a union reports missing fields from multiple members.
  // Facilitates cleaner errors for mismatches against union schemas.
  // `missing` should not be empty, and its elements should not be empty.
  final case class UnionMissingFields(path: Path, missing: Seq[Seq[MissingField]])
      extends FieldConstraintFailure {

    def msg: String = {
      val messages = missing.map(_.map(_.field).mkString("[", ", ", "]"))
      if (messages.sizeIs == 1) {
        // Can't really happen but we might as well write something.
        s"Union requires fields ${messages.head}"
      } else if (messages.sizeIs == 2) {
        // E.g. "[terms] or [values]"
        s"Union requires fields ${messages(0)} or ${messages(1)}"
      } else {
        // E.g. "[terms], [values], or [terms, values]"
        val init = messages.init.mkString(", ")
        s"Union requires fields $init, or ${messages.last}"
      }
    }
  }

  // When a custom validator reports a failure.
  final case class ValidatorFailure(path: Path, msg: String)
      extends FieldConstraintFailure

  final case class UniqueConstraintFailure(fields: List[Path])
      extends ConstraintFailure

  final case class CheckConstraintFailure(name: String, msg: String)
      extends ConstraintFailure {
    def fields = Nil
  }

  object CheckConstraintFailure {
    def Rejected(name: String) =
      CheckConstraintFailure(name, s"Document failed check constraint `$name`")
    def BadReturn(name: String) = CheckConstraintFailure(
      name,
      s"Error evaluating check constraint `$name`: function returned a non-boolean value")
    def Error(name: String, msg: String) = CheckConstraintFailure(
      name,
      s"Error evaluating check constraint `$name`: $msg")
  }

  final case class DefaultValueFailure(path: Path, msg: String)
      extends FieldConstraintFailure

  final case class FQL4UniqueConstraintFailure(indexName: String)
      extends ConstraintFailure {
    override def label = indexName
    def fields = Nil
  }

  final case class TenantRootWriteFailure(collName: String)
      extends ConstraintFailure {
    override def label = ""
    def fields = Nil
  }

  final case class StagedDeletionFailure(collName: String)
      extends ConstraintFailure {
    def fields = Nil
  }

  final case class StagedRenameFailure(collName: String) extends ConstraintFailure {
    def fields = Nil
  }

  final case class ProtectedModeFailure(msg: String) extends ConstraintFailure {
    def fields = Nil
  }

  object ProtectedModeFailure {
    private val suffix =
      "destructive change forbidden because database is in protected mode."

    private def pmf(prefix: String) = ProtectedModeFailure(s"$prefix: $suffix")

    val DeleteCollection = pmf("Cannot delete collection")
    val DeleteBackingIndex = pmf(s"Cannot cause deletion of backing index")

    private def decreaseField(field: String) = pmf(s"Cannot decrease `$field` field")
    val DecreaseHistoryDays = decreaseField("history_days")
    val DecreaseTTLDays = decreaseField("ttl_days")

    val RemoveHistoryDays = pmf("Cannot remove `history_days` field")
    val AddTTLDays = pmf("Cannot remove `ttl_days` field")
  }
}
