package fauna.atoms

import scala.annotation.unused

sealed abstract class APIVersion(
  final val ordinal: Int,
  override val toString: String
) extends Ordered[APIVersion] {

  APIVersion._versions += this

  def compare(that: APIVersion) = Integer.compare(this.ordinal, that.ordinal)

  def max(other: APIVersion) =
    if (this < other) other else this
}

object APIVersion {
  private var _versions = Set.empty[APIVersion]

  final val V20 = new APIVersion(1, "2.0") {}
  final val V21 = new APIVersion(2, "2.1") {}
  final val V27 = new APIVersion(3, "2.7") {}
  final val V212 = new APIVersion(4, "2.12") {}
  final val V3 = new APIVersion(5, "3") {}
  final val V4 = new APIVersion(6, "4") {}
  final val V5 = new APIVersion(7, "5") {}

  // A catchall API version for unreleased, unsupported functionality.
  final val Unstable = new APIVersion(Int.MaxValue, "UNSTABLE") {}

  // APIVersion used for requests with no API Version header set
  final val Default = V21

  // APIVersion after which stored QueryV are written with an API version, so
  // they can be deserialized to the correct AST
  final val LambdaDefaultVersion = V212

  // All APIVersions must be created before this line
  final val Versions: Set[APIVersion] = _versions

  private[this] val ByOrdinal: Map[Int, APIVersion] =
    (Versions map { v => v.ordinal -> v }).toMap

  private[this] val ByStringRepr: Map[String, APIVersion] =
    (Versions map { v => v.toString -> v }).toMap

  def apply(ord: Int): APIVersion = ByOrdinal.getOrElse(ord, Default)
  def unapply(version: String): Option[APIVersion] = ByStringRepr.get(version)

  def removedOn(version: APIVersion): APIVersion => Boolean = _ < version
  def introducedOn(version: APIVersion): APIVersion => Boolean = _ >= version

  // Just a tag to indicate the function was deprecated, doesn't do any version check
  def deprecatedOn(@unused version: APIVersion): APIVersion => Boolean = _ => true
}
