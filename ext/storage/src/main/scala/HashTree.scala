package fauna.storage

import fauna.atoms._
import fauna.codex.cbor._
import fauna.lang.syntax._
import java.util.{ Arrays, LinkedList }
import scala.jdk.CollectionConverters._

object HashTree {
  /**
    * Returns a new builder for a HashTree with at most `2^maxDepth`
    * nodes, containing the hash of rows within a `segment` of the
    * token space.
    */
  def newBuilder(segment: Segment, maxDepth: Int): HashTree.Builder =
    new Builder(segment, maxDepth)

  class Builder(segment: Segment, maxDepth: Int) {

    private[this] var last = Location.MinValue
    private[this] var tree: Node = Leaf(segment)

    /**
      * Insert a block hash into the tree corresponding to the
      * provided `location`. This method may be called multiple times
      * with the same `location`, but must always be called in
      * ascending Location order.
      *
      * The hash should be computed using a suitably strong hash
      * function; the tree uses a simple XOR to mix hashes.
      */
    def insert(location: Location, hash: Array[Byte]): Builder = {
      require(segment contains location, s"$location is not contained in this tree")
      require(location >= last, s"row is out of order: $location < $last") // here come da judge

      def insert0(root: Node, depth: Byte): Node = {
        root match {
          case Leaf(seg, hsh) if depth < maxDepth && hsh.isEmpty =>
            val mid = seg.midpoint

            if (mid == seg.left || mid == seg.right) {
              val right = if (mid == Location.MaxValue) {
                Location.MinValue
              } else {
                Location(mid.token + 1)
              }

              Leaf(Segment(mid, right), hash)
            } else {
              insert0(Inner(Leaf(seg.copy(right = mid)), Leaf(seg.copy(left = mid))), inc(depth))
            }

          case Inner(left, right) =>
            // FIXME: make this tailrec
            if (left.contains(location)) {
              Inner(insert0(left, inc(depth)), right)
            } else {
              Inner(left, insert0(right, inc(depth)))
            }

          case Leaf(_, _) =>
            root.rehash(location, hash)
        }
      }

      tree = insert0(tree, 0)
      last = location
      this
    }

    def result: HashTree = HashTree(segment, tree)

    private def inc(in: Byte): Byte = {
      assert(in < Byte.MaxValue)
      (in + 1).toByte
    }
  }

  sealed trait Node {
    val segment: Segment

    private[HashTree] def hash: Array[Byte]

    private[HashTree] def rehash(location: Location, bytes: Array[Byte]): Node

    def contains(location: Location): Boolean =
      segment contains location

    def matches(other: Node): Boolean =
      Arrays.equals(hash, other.hash)
  }

  object Node {
    implicit val CBORCodec: CBOR.Codec[Node] =
      CBOR.SumCodec(
        CBOR.TupleCodec[Leaf],
        CBOR.TupleCodec[Inner])
  }

  case class Leaf(
    segment: Segment,
    private[HashTree] var hash: Array[Byte] = Array.emptyByteArray)
      extends Node {

    private[HashTree] def rehash(l: Location, bytes: Array[Byte]): Node = {
      require(contains(l), s"incorrect hash location $l for $this")
      hash = hash xor bytes
      this
    }

    override def toString = s"Leaf($segment, ${hash.toHexString})"
  }

  case class Inner(left: Node, right: Node) extends Node {

    lazy val segment: Segment =
      Segment(left.segment.left, right.segment.right)

    // NOTE: we are conceivably at risk of a preimage attack, because
    // the tree's topology is not encoded in the XOR hash. For
    // instance, it is possible for a 'real' hash tree representing
    // data on disk to have the same root hash as a synthetic tree
    // representing invalid data.
    private[HashTree] def hash: Array[Byte] = left.hash xor right.hash

    private[HashTree] def rehash(loc: Location, bytes: Array[Byte]): Node = {
      if (left.contains(loc)) {
        left.rehash(loc, bytes)
      } else {
        right.rehash(loc, bytes)
      }

      this
    }

    override def toString = s"Inner($left, $right)"
  }

  implicit val CBORCodec = CBOR.TupleCodec[HashTree]

}

/**
  * Represents a hash tree (aka Merkle tree) over a segment of the
  * token space.
  *
  * In a perfect version of this tree, each leaf represents a single
  * token within the segment, and a hash of its contents on disk. A
  * perfect tree over the entire token space would be prohibitively
  * large, so we bound the depth of the tree based on the level of
  * detail required during differencing.
  *
  * The root hash may be used to determine whether
  * two processes contain the same data for the segment. If more
  * detailed differences need to be detected - such as which specific
  * blocks are different - a hash tree deep enough to reach the level
  * of detail required can be built. See the `maxSize` option to
  * `HashTree.newBuilder`.
  *
  * Once a tree has been built using a `HashTree.Builder`, it should
  * never be modified.
  */
case class HashTree(segment: Segment, private[HashTree] val root: HashTree.Node) {

  /**
    * Returns the hash of all bytes stored within the tree's segment.
    */
  def hash: Array[Byte] = root.hash

  /**
    * Returns a list of segments for which this and `other` disagree.
    */
  def diff(other: HashTree): Iterable[Segment] = {
    require(segment == other.segment, "hash trees must cover the same segment")

    def diff0(ref: HashTree.Node, cmp: HashTree.Node, diff: LinkedList[Segment]): LinkedList[Segment] =
      (ref, cmp) match {
        case (a: HashTree.Inner, b: HashTree.Inner) =>
          if (a matches b) {
            diff
          } else {
            val left = diff0(a.left, b.left, diff)
            diff0(a.right, b.right, left)
          }
        case (a, b) =>
          if (a matches b) {
            diff
          } else {
            // we are executing a pre-order traversal, so we will see
            // locations in ascending order. therefore, we can union
            // contiguous segments to arrive at the minimum number of
            // segments covering the diff
            Option(diff.peekLast) match {
              case Some(e) if e.right == a.segment.left =>
                diff.addLast(Segment(diff.removeLast.left, a.segment.right))
              case _ =>
                diff.addLast(a.segment)
            }
            diff
          }
      }

    if (root matches other.root) {
      Nil
    } else {
      diff0(root, other.root, new LinkedList[Segment]).asScala
    }
  }

}
