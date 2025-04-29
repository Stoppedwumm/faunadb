package fauna.storage.doc

import fauna.codex.cbor._
import fauna.storage.ir._
import scala.annotation.tailrec
import scala.language.implicitConversions

case class FieldPair(path: List[String], value: IRValue)

object FieldPair {
  implicit def fieldToFP[T, V](kv: (Field[T], V))(implicit cast: V => T) =
    FieldPair(kv._1.path, kv._1.ftype.encode(kv._2) getOrElse NullV)
}

sealed trait AbstractData extends Any {
  def fields: MapV

  def update(pair: FieldPair, pairs: FieldPair*): AbstractData

  def isEmpty = fields.isEmpty
}

object Data {

  implicit val codec = CBOR.AliasCodec[Data, MapV](apply, _.fields)

  val empty = Data(MapV.empty)

  def apply(pairs: FieldPair*): Data =
    if (pairs.isEmpty) empty else empty.update(pairs.head, pairs.tail: _*)
}

case class Data(fields: MapV) extends AnyVal with AbstractData {

  def apply[T](field: Field[T]): T = field(fields)

  def getOrElse[T](field: Field[T], orElse: => T): T =
    getOpt(field).getOrElse(orElse)

  def getOpt[T](field: Field[T]): Option[T] =
    field.read(fields).toOption

  def update(pair: FieldPair, pairs: FieldPair*): Data =
    Data(((pair +: pairs) foldLeft fields) { (m, p) =>
      if (p.value == NullV) m.remove(p.path) else m.update(p.path, p.value)
    })

  def remove[T](field: Field[T]): Data =
    if (fields.contains(field.path)) {
      copy(fields = fields.remove(field.path))
    } else {
      this
    }

  /** Combine two structs, favoring values on the right.
    */
  def merge(other: Data): Data = Data(fields merge other.fields)

  /** Return a struct containing the values that are not present or
    * different from the right
    */
  def subtract(other: Data): Data = Data(fields subtract other.fields)

  /** Apply a patch and return the revised struct.
    */
  def patch(diff: Diff): Data = if (diff.isEmpty) this
  else {
    def patch0(value: IRValue): Option[IRValue] = value match {
      case NullV                        => None
      case v @ (_: ScalarV | _: QueryV) => Some(v)
      case ArrayV(elems) =>
        val patchedElems = Vector.newBuilder[IRValue]
        elems foreach { v =>
          patch0(v) match {
            case Some(patchedValue) => patchedElems.addOne(patchedValue)
            case None               => ()
          }
        }
        Some(ArrayV(patchedElems.result()))
      case MapV(pairs) =>
        val patchedPairs = List.newBuilder[(String, IRValue)]
        pairs foreach { case (k, v) =>
          patch0(v) match {
            case Some(patchedValue) => patchedPairs.addOne(k -> patchedValue)
            case None               => ()
          }
        }
        Some(MapV(patchedPairs.result()))
    }

    Data((fields combine diff.fields) {
      case (Some(l @ MapV(_)), Some(r @ MapV(_))) =>
        Some((Data(l) patch Diff(r)).fields)
      case (_, Some(r)) => patch0(r)
      case (l, _)       => l
    })
  }

  /** Generate a diff that would turn the lhs into the rhs.
    */
  def diffTo(data: Data): Diff = if (isEmpty) Diff(data.fields)
  else {
    Diff((fields combine data.fields) {
      case (Some(l @ MapV(_)), Some(r @ MapV(_))) =>
        MapV.lift((Data(l) diffTo Data(r)).fields)
      case (Some(l), sr @ Some(r)) => if (l != r) sr else None
      case (None, sr @ Some(_))    => sr
      case (_, None)               => Some(NullV)
    })
  }

  /** Given a diff, return a diff that reverses its changes to this struct.
    */
  def inverse(diff: Diff): Diff = (this patch diff) diffTo this

  /** Perform an order-insensitive comparison of two structs.
    */
  def sameElements(data: Data): Boolean = (this diffTo data).isEmpty

  /** Recursively removes null values in objects.
    */
  def elideNulls: Data = {
    def elide0(ir: IRValue): IRValue =
      ir match {
        case MapV(fields) =>
          MapV(fields.flatMap {
            case (_, NullV) => None
            case (n, ir)    => Some(n -> elide0(ir))
          })
        case ArrayV(elems) => ArrayV(elems.map(elide0))
        case ir            => ir
      }
    Data(elide0(fields).asInstanceOf[MapV])
  }

  // This function will insert a value, without overwriting any existing fields.
  //
  // Aside from the `data` field at the start of the path, all parent fields of
  // `path` must exist for this function to work.
  //
  // For example, with the path `.data.foo` and the value `3`:
  // - given   { data: {} }
  //   returns { data: { foo: 3 } }
  // - given   {}
  //   returns { data: { foo: 3 } }
  //
  // However, with the path .data.foo.bar = 3
  // - given   { data: { foo: {} } }
  //   returns { data: { foo: { bar: 3 } } }
  // - given   { data: {} }
  //   error! the field `foo` is missing.
  // - given   { data: { foo: 3 } }
  //   error! the field `foo` is not a struct.
  def insert(path: ConcretePath, value: IRValue): Data = {
    val dataPath = path.toDataPath
    if (fields.contains(dataPath)) {
      throw new IllegalStateException(
        s"Refusing to overwrite existing field at $dataPath")
    }

    // The `data` field is special, because it might not exist yet. So we start by
    // validating one element into the path.
    var current = fields
    val slice = if (dataPath.sizeIs > 1 && dataPath.head == "data") {
      current = fields.elems.find(_._1 == "data") match {
        case Some((_, v: MapV)) => v
        case _                  => MapV.empty
      }

      // `drop(1)` skips the `.data` field.
      dataPath.drop(1)
    } else {
      dataPath
    }

    for (elem <- slice.dropRight(1)) {
      current = current.elems.find(_._1 == elem) match {
        case Some((_, v: MapV)) => v
        case _ =>
          throw new IllegalStateException(
            s"Refusing to overwrite non-struct parent of nested field at $dataPath")
      }
    }

    Data(fields.update(dataPath, value))
  }

  // Removes the path from the data, returning the data without the given path.
  //
  // This will not remove parent structs of the path removed. It will leave empty
  // structs in place.
  //
  // If the path points to something that doesn't exist, the original data is
  // returned.
  //
  // If the path points to a non-struct somewhere in the middle, an
  // IllegalStateException is thrown.
  //
  // `data` is special, and it will be removed if its the only parent of the field
  // being removed.
  def remove(path: ConcretePath): Data = {
    val dataPath = path.toDataPath

    def modify(m: MapV, path: List[String]): MapV = {
      val rv = path match {
        case Nil => AList(m.elems)

        case key :: Nil => AList(m.elems).modify(key, _ => None)

        case key :: rest =>
          AList(m.elems).modify(
            key,
            (e) =>
              e match {
                case Some(m: MapV) =>
                  val m0 = modify(m, rest)

                  // Remove the `data` field if its empty.
                  if (
                    path.length == dataPath.length && path.head == "data" && m0.isEmpty
                  ) {
                    None
                  } else {
                    Some(m0)
                  }

                case other =>
                  // `data` is allowed to be missing.
                  if (
                    other == None && path.length == dataPath.length && path.head == "data"
                  ) {
                    None
                  } else {
                    throw new IllegalStateException(
                      "Refusing to remove path whose parent is not a struct")
                  }
              }
          )
      }

      MapV(rv.elems)
    }

    Data(modify(fields, dataPath))
  }

  // Returns the value at the given path, if it exists.
  //
  // If the value somewhere in the middle of the path is not a struct, an
  // IllegalStateException is thrown.
  //
  // `data` is special, and it will behave like an empty struct if it doesn't exist.
  def get(path: ConcretePath): Option[IRValue] = {
    val dataPath = path.toDataPath

    @tailrec
    def get(v: Option[IRValue], path: List[String]): Option[IRValue] =
      (v, path) match {
        case (v, Nil) => v
        case (Some(MapV(elems)), key :: rest) =>
          val value = AList(elems).get(key)

          if (value == None && key == "data" && path.length == dataPath.length) {
            // `data` acts like its always there.
            get(Some(MapV()), rest)
          } else {
            get(value, rest)
          }

        case _ =>
          throw new IllegalStateException(
            "Cannot read field whose parent is not a struct")
      }

    get(Some(fields), dataPath)
  }
}

object Diff {
  implicit val codec = CBOR.AliasCodec[Diff, MapV](apply, _.fields)

  val empty = Diff(MapV.empty)

  def apply(pairs: FieldPair*): Diff =
    if (pairs.isEmpty) empty else empty.update(pairs.head, pairs.tail: _*)
}

case class Diff(fields: MapV) extends AnyVal with AbstractData {

  def update(pair: FieldPair, pairs: FieldPair*): Diff =
    Diff(((pair +: pairs) foldLeft fields) { (m, p) =>
      m.update(p.path, p.value)
    })

  def clear[T](field: Field[T]): Diff = Diff(fields.remove(field.path))

  /** Merge two diffs. Returns a diff that is equivalent to applying
    * the left and then right diff.
    */
  def andThen(diff: Diff): Diff = Diff(fields merge diff.fields)

  /** Return the set of changes that are not applied by the right
    * diff.
    */
  def subtract(diff: Diff): Diff = Diff(fields subtract diff.fields)
}
