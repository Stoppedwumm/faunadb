package fauna.qa

import fauna.codex.json.JSObject
import fauna.prop.{ Prop, PropConfig }

/**
  * Used to remember a random subset of known objects.
  */
final class SamplingBuffer(capacity: Integer)(implicit propConfig: PropConfig) {
  private[this] val buffer = new Array[JSObject](capacity)

  @volatile private[this] var _size = 0

  /**
    * Returns the current number of objects in the buffer.
    */
  def size = _size

  /**
    * Adds the given objects to the buffer. If the buffer is at
    * capacity, inserts each object at a randomly-assigned position in
    * the buffer.
    */
  def add(objs: Iterable[JSObject]) = synchronized {
    objs foreach { obj =>
      if (_size < capacity) {
        buffer(_size) = obj
        _size += 1
      } else {
        buffer(Prop.int(capacity).sample) = obj
      }
    }
  }

  /**
    * Removes and returns at most `count` objects from the buffer.
    */
  def take(count: Int): Seq[JSObject] = synchronized {
    (0 until (count min _size)) map { _ =>
      val idx = Prop.int(_size min capacity).sample
      val obj = buffer(idx)
      _size -= 1
      val remove = _size min (capacity - 1)
      buffer(idx) = buffer(remove)
      buffer(remove) = null
      obj
    } toList
  }
}
