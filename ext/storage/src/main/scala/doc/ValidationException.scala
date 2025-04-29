package fauna.storage.doc

import fauna.atoms.{ DocID, ScopeID }
import fauna.lang.syntax._
import fauna.storage.ir.IRType
import scala.runtime.ScalaRunTime

//FIXME: These really should not be exceptions.
sealed trait ValidationException extends Exception {
  def path: List[String]
  def code: String
  def jsPath = path.asJSPath

  override def fillInStackTrace() = this

  override def getMessage(): String =
    this match {
      case p: Product => ScalaRunTime._toString(p)
      case _          => super.getMessage()
    }
}

case class ValueRequired(path: List[String])
    extends NoSuchElementException
    with ValidationException {
  def code = "value required"
}

case class FieldNotAllowed(path: List[String]) extends ValidationException {
  def code = "invalid field"
}

case class InvalidType(
  path: List[String],
  expected: IRType,
  actual: IRType,
  context: Option[(ScopeID, DocID)] = None)
    extends ValidationException {
  def code = "invalid type"
}

case class MultipleWildcards(path: List[String]) extends ValidationException {
  def code = "multiple wildcards"
}

case class MultipleClassBindings(path: List[String]) extends ValidationException {
  def code = "multiple bindings"
}

case class MixedWildcards(path: List[String]) extends ValidationException {
  def code = "mixed wildcards"
}

case class DuplicateValue(path: List[String]) extends ValidationException {
  def code = "duplicate value"
}

case class OutOfBoundValue(path: List[String], actual: Long, min: Long, max: Long)
    extends ValidationException {
  def code = "bounds error"
}

case class InvalidNegative(path: List[String]) extends ValidationException {
  def code = "non-negative value required"
}

case class MissingField(path: List[String], alternatives: List[String]*)
    extends ValidationException {
  def code = "value required"
}

case class DuplicateCachedValue(path: List[String], ttlSecs: Int)
    extends ValidationException {
  def code = "duplicate value"
}

case class InvalidReference(path: List[String]) extends ValidationException {
  def code = "invalid reference"
}

case class InvalidScopedReference(path: List[String]) extends ValidationException {
  def code = "invalid reference"
}

case class InaccessibleReference(path: List[String]) extends ValidationException {
  def code = "invalid reference"
}

case class ReferenceMissing(path: List[String]) extends ValidationException {
  def code = "invalid reference"
}

case class InvalidSourceBinding(path: List[String]) extends ValidationException {
  def code = "invalid binding"
}

case class UnusedSourceBinding(path: List[String]) extends ValidationException {
  def code = "unused binding"
}

sealed trait IndexShapeChange extends ValidationException {
  def path = Nil
  def code = "invalid index"
}

object IndexShapeChange {
  case class Partition(orig: Long, proposed: Long) extends IndexShapeChange

  case class Fields(diff: Diff) extends IndexShapeChange
}

case class InvalidArity(path: List[String], expected: String, actual: String)
    extends ValidationException {
  def code = "invalid arity"
}

case class ResourcesExceeded(path: List[String], resource: String, limit: Int)
    extends ValidationException {
  def code = "resources exceeded"
}

case class ReservedName(
  path: List[String],
  name: CharSequence,
  reserved: Set[CharSequence])
    extends ValidationException {
  def code = "reserved name"
}

case class InvalidCharacters(
  path: List[String],
  name: CharSequence,
  validRegex: String)
    extends ValidationException {
  def code = "invalid characters"
}

case class InvalidTransformValue(path: List[String], str: String)
    extends ValidationException {
  def code = "invalid value"
}

case class InvalidPassword(path: List[String]) extends ValidationException {
  def code = "invalid password"
}

case class InvalidSecret(path: List[String]) extends ValidationException {
  def code = "invalid secret"
}

case class ContainerCandidateContainsData(path: List[String])
    extends ValidationException {
  def code = "container candidate contains data"
}

case class InvalidURI(path: List[String], uri: String) extends ValidationException {
  def code = "invalid uri"
}

case class ValidationFailure(path: List[String], msg: String)
    extends ValidationException {
  def code = "validation failure"
}
