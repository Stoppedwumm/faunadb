package fauna.storage.doc

import fauna.storage.ir._
import scala.collection.immutable.ArraySeq

/** ConcretePath represents a path to a field in a document.
  *
  * These paths will always be an absolute path. So for user-documents, the
  * path will typically start with `data` (all the v10 path handling is done
  * at a higher level).
  *
  * For example, a migration to move the field `foo` to `bar` (in a
  * user-defined collection) would have two paths:
  * - from: `ConcretePath("data", "foo")`
  * -   to: `ConcretePath("data", "bar")`
  *
  * When this migration is applied, it will operate on `Data` that looks
  * approximately like this:
  * ```
  * {
  *   ref: Ref("123", Collection("Foo")), // if you're feeling like using v4
  *   id: "123",                          // if you're feeling like using v10
  *   coll: Foo,                          // if you're feeling like using v10
  *   ts: Time("2021-01-01T00:00:00Z"),
  *   data: {
  *     foo: "some value"
  *     // ...
  *   }
  * }
  * ```
  *
  * The migration will then produce:
  * ```
  * {
  *   // these are unchanged, migrations will not touch them.
  *   ref: Ref("123", Collection("Foo")), // if you're feeling like using v4
  *   id: "123",                          // if you're feeling like using v10
  *   coll: Foo,                          // if you're feeling like using v10
  *   ts: Time("2021-01-01T00:00:00Z"),
  *   data: {
  *     bar: "some value" // this is changed!
  *     // ...
  *   }
  * }
  * ```
  */
final case class ConcretePath(path: ArraySeq[String]) extends AnyVal {
  def toDataPath: List[String] = if (path.isEmpty) {
    throw new IllegalStateException(s"Unsupported path: ${path.toList}")
  } else {
    path.toList
  }

  def concat(other: ConcretePath): ConcretePath = ConcretePath(path ++ other.path)
  def append(elem: String): ConcretePath = ConcretePath(path :+ elem)

  def encode = ArrayV(path.map(StringV(_)).toVector)
}

object ConcretePath {
  def apply(path: String*): ConcretePath = ConcretePath(path.to(ArraySeq))

  def decode(v: ArrayV) = ConcretePath(
    v.elems
      .map {
        case StringV(v) => v
        case v => throw new IllegalStateException(s"invalid path element $v")
      }
      .to(ArraySeq))
}
