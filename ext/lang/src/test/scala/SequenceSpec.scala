package fauna.lang.test

import fauna.lang.Monad
import fauna.lang.syntax._

class SequenceSpec extends Spec {
  "sequence" - {
    "works on Option" in {
      // Manually making monad tc instance for List since Query isn't accessible from here
      implicit object MonadInstance extends Monad[List] {
        def pure[A](a: A) = List(a)
        def map[A, B](m: List[A])(f: A => B) = m map f
        def flatMap[A, B](m: List[A])(f: A => List[B]) = m flatMap f
        def accumulate[A, B](ms: Iterable[List[A]], seed: B)(f: (B, A) => B) = ???
      }

      val someL: Option[List[Int]] = Some(List(1))
      someL.sequence shouldBe List(Option(1))

      val none: Option[List[Int]] = None
      none.sequence shouldBe List(None)
    }
  }
}
