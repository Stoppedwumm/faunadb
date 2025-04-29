package fauna.lang

import java.util.{ Random => JRandom }
import scala.language.implicitConversions
import scala.util.Random

trait RandomSyntax {
  implicit def asRichRandom(r: Random): RandomSyntax.ScalaRichRandom =
    RandomSyntax.ScalaRichRandom(r)

  implicit def asRichRandom(r: JRandom): RandomSyntax.JavaRichRandom =
    RandomSyntax.JavaRichRandom(r)
}

object RandomSyntax {
  case class ScalaRichRandom(r: Random) extends AnyVal {
    def choose[A](s: Seq[A]): A =
      if (s.isEmpty) {
        throw new NoSuchElementException("choice from an empty collection")
      } else {
        s(r.nextInt(s.size))
      }

    def chooseOption[A](s: Seq[A]): Option[A] =
      Option.unless(s.isEmpty)(choose(s))
  }

  case class JavaRichRandom(r: JRandom) extends AnyVal {
    def choose[A](s: Seq[A]): A =
      if (s.isEmpty) {
        throw new NoSuchElementException("choice from an empty collection")
      } else {
        s(r.nextInt(s.size))
      } 

    def chooseOption[A](s: Seq[A]): Option[A] =
      Option.unless(s.isEmpty)(choose(s))
  }
}
