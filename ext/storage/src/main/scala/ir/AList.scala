package fauna.storage.ir

import scala.collection.mutable.{ Map => MMap }

case class AList[+T](elems: List[(String, T)]) extends AnyVal {
  def hasSameElementsAs[T0 >: T](other: AList[T0]): Boolean = (elems sortBy { _._1 }) == (other.elems sortBy { _._1 })

  def get(key: String): Option[T] = {
    @annotation.tailrec
    def get0(l: List[(String, T)], key: String): Option[T] = l match {
      case ((k, v) :: tail) => if (key == k) Some(v) else get0(tail, key)
      case Nil              => None
    }

    get0(elems, key)
  }

  def modify[T0 >: T](key: String, f: Option[T] => Option[T0]): AList[T0] = {
    def modify0(l: List[(String, T)]): List[(String, T0)] = l match {
      case (k, v) :: tail if key == k =>
        f(Some(v)) match { case Some(v0) => (k -> v0) :: tail; case _ => tail }
      case t :: tail => t :: modify0(tail)
      case Nil       => f(None) match { case Some(v0) => List(key -> v0); case _ => Nil }
    }

    AList(modify0(elems))
  }

  def combine[T0, T1](other: AList[T0])(f: (Option[T], Option[T0]) => Option[T1]): AList[T1] = {
    val rem = MMap(other.elems: _*)
    val rv = List.newBuilder[(String, T1)]

    elems foreach { case (k, v) => f(Some(v), rem remove k) foreach { v0 => rv += (k -> v0) } }
    other.elems foreach { case (k, v) => if (rem contains k) f(None, Some(v)) foreach { v0 => rv += (k -> v0) } }

    AList(rv.result())
  }
}
