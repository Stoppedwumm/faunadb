package fauna.lang

object CartesianSeq {

  /**
    * This additional constructor for CartesianSeq simply optimizes
    * the case where any of the input sequences is empty, therefore
    * the product is also empty.
    */
  def apply[T](axes: IndexedSeq[IndexedSeq[T]]): Seq[Seq[T]] = {
    if (axes exists { _.isEmpty }) {
      return Seq.empty
    }

    new CartesianSeq(axes)
  }

  private def badIndex(index: Int, len: Int): String = {
    if (index < 0) {
      return s"index ($index) must not be negative"
    } else if (len < 0) {
      throw new IllegalArgumentException("negative length: " + len)
    } else { // index >= len
      return s"index ($index) must be less than length ($len)"
    }
  }
}

/**
  * This class yields the cartesian product of a set of sequences.
  *
  * Allocations are minimized in the process by relying on the
  * immutability of the input and yielding each item in each
  * sub-sequence upon request.
  *
  * No copies of the input are created.
  */
final class CartesianSeq[T] private(private[this] val axes: IndexedSeq[IndexedSeq[T]])
    extends IndexedSeq[Seq[T]] {

  private[this] val axesLengthProduct = {
    val array = new Array[Int](axes.length + 1)

    array(axes.length) = 1

    for (i <- (axes.length - 1) to 0 by -1) {
      val mul = array(i + 1) * axes(i).length.toLong

      if (mul == mul.toInt) { // Overflow check.
        array(i) = mul.toInt
      } else {
        throw new IllegalArgumentException(
          s"Cartesian product too large. ($mul > ${Int.MaxValue})")
      }
    }

    array
  }

  def length: Int = axesLengthProduct(0)

  def apply(index: Int): Seq[T] = {
    if (index < 0 || index >= length) {
      val msg = CartesianSeq.badIndex(index, length)
      throw new IndexOutOfBoundsException(msg)
    }

    return new IndexedSeq[T] {
      override def length: Int = axes.length

      override def apply(axis: Int): T = {
        val i = axisIndexForProductIndex(index, axis)
        axes(axis)(i)
      }
    }
  }

  private def axisIndexForProductIndex(index: Int, axis: Int) =
    (index / axesLengthProduct(axis + 1)) % axes(axis).length
}
