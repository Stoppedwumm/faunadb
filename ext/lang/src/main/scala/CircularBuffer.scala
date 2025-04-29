package fauna.lang

import scala.reflect.ClassTag

object CircularBuffer {
  def apply[T: ClassTag](size: Int) = {
    require(size >= 0, "size must be non-negative.")
    new CircularBuffer(new Array[T](size: Int), new Array[T](1)(0))
  }
}

class CircularBuffer[T](arr: Array[T], nullValue: T) {
  private[this] var _size = 0
  private[this] var offset = 0

  def size = _size

  def capacity = arr.length

  def isEmpty = size == 0
  def head = if (isEmpty) throw new NoSuchElementException else apply(0)
  def last = if (isEmpty) throw new NoSuchElementException else apply(size - 1)

  def apply(i: Int) =
    if ((i < 0) || (i >= size)) {
      throw new ArrayIndexOutOfBoundsException(i)
    } else {
      arr((offset + i) % capacity)
    }

  def update(i: Int, e: T) =
    if ((i < 0) || (i >= size)) {
      throw new ArrayIndexOutOfBoundsException(i)
    } else {
      arr((offset + i) % capacity) = e
    }

  def add(e: T) = {
    val i = (offset + size) % capacity

    if (size < capacity) {
      _size += 1
    } else {
      offset = (offset + 1) % capacity
    }

    arr(i) = e
  }

  def +=(e: T) = add(e)

  def shrink(newSize: Int) = {
    require(newSize <= size)

    var i = newSize

    while (i < size) {
      update(i, nullValue)
      i += 1
    }

    _size = newSize
  }

  def remove(i: Int) = {
    if ((i < 0) || (i >= size)) {
      throw new ArrayIndexOutOfBoundsException(i)
    }

    var idx = i
    while ((idx + 1) < size) {
      update(idx, apply(idx + 1))
      idx += 1
    }

    shrink(size - 1)
  }

  def filtered(f: T => Boolean): Unit = {
    var i = 0
    var nxt = 0

    while (i < size) {
      val v = apply(i)
      if (f(v)) {
        if (i != nxt) update(nxt, v)
        nxt += 1
      }
      i += 1
    }

    shrink(nxt)
  }

  def foreach[U](f: T => U): Unit = {
    var i = 0

    while (i < size) {
      f(apply(i))
      i += 1
    }
  }

  def toSeq = {
    val b = Seq.newBuilder[T]
    foreach { b += _ }
    b.result()
  }

  def mkString(start: String, sep: String, end: String) = {
    val b = new StringBuilder

    b ++= start

    foreach { e =>
      b ++= e.toString
      b ++= sep
    }

    b ++= end

    b.result()
  }
}
