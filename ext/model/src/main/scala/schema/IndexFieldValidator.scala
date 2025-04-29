package fauna.model.schema

import fauna.model.schema.FieldPath
import fauna.repo.query.Query
import fauna.repo.schema.{ ConstraintFailure, Path }
import fauna.repo.schema.FieldSchema.FieldValidator
import fauna.storage.ir.{ ArrayV, MapV, StringV }

object IndexFieldValidator {

  // TODO: We cannot properly index `id` and `coll` because they do
  // have a proper storage representation. Attempting to index these off a
  // nested ref will silently fail, but we outright reject them here for the top
  // level. It should be possible to index `id`, but we'll need to figure out
  // how to translate to/from a persisted format w/o losing information. Because
  // id and coll are already indexed and available, opting to not support this
  // to start. Can re-evaluate based on customer feedback if need be.
  private val DisallowedIndexTerms = Map(
    "id" -> "`id` is not supported as an index term, use the native byId method to look up a document by its id.",
    "coll" -> "`coll` is not supported as an index term.",
    "ts" -> "`ts` may only be used as an index value, not as an index term."
  )

  private val DisallowedIndexValues = Map(
    "coll" -> "`coll` is not supported as a specified index value."
  )

  private val DisallowedUniqueValues = Map(
    "id" -> "`id` is not supported as a unique constraint value.",
    "coll" -> "`coll` is not supported as a unique constraint value.",
    "ts" -> "`ts` is not supported as a unique constraint value."
  )

  /** field path validators */

  val termValidator: FieldValidator = pathValueValidator(DisallowedIndexTerms)

  val valueValidator: FieldValidator = pathValueValidator(DisallowedIndexValues)

  val uniqueConstraintValidator: FieldValidator = { case (prefix, arg, fromValue) =>
    def oops(expected: String) = throw new IllegalStateException(
      s"Unexpected type for field $prefix, expected $expected, received toValue: $arg, fromValue: $fromValue")

    val b = List.newBuilder[ConstraintFailure.FieldConstraintFailure]

    arg match {
      case ArrayV(elems) =>
        elems foreach {
          case StringV(p) => b ++= validatePath(prefix, DisallowedUniqueValues, p)
          case MapV(fields) =>
            fields.find({ case (f, _) => f == "field" }) foreach {
              case (_, StringV(p)) =>
                b ++= validatePath(prefix, DisallowedUniqueValues, p)
              case _ => oops("StringV")
            }
          case _ => oops("StringV or MapV")
        }
      case _ => oops("ArrayV")
    }

    Query.value(b.result())
  }

  private def validatePath(
    prefix: Path.Prefix,
    disallowed: Map[String, String],
    path: String) =
    path match {
      case "" =>
        Seq(
          ConstraintFailure
            .ValidatorFailure(prefix.toPath, "Value cannot be empty."))

      case FieldPath(Right(name) :: _) if disallowed.contains(name) =>
        Seq(
          ConstraintFailure
            .ValidatorFailure(prefix.toPath, disallowed(name)))

      case FieldPath(_) => Nil

      case str =>
        Seq(
          ConstraintFailure.ValidatorFailure(
            prefix.toPath,
            s"Value `$str` is not a valid FQL path expression."))
    }

  private def pathValueValidator(disallowed: Map[String, String]): FieldValidator = {
    case (prefix, arg, fromValue) =>
      def oops() = throw new IllegalStateException(
        s"Unexpected type for field $prefix, expected StringV, received toValue: $arg, fromValue: $fromValue")

      val b = List.newBuilder[ConstraintFailure.FieldConstraintFailure]

      arg match {
        case StringV(p) =>
          b ++= validatePath(prefix, disallowed, p)
        case _ => oops()
      }

      Query.value(b.result())
  }
}
