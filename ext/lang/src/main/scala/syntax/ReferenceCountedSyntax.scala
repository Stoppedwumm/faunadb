package fauna.lang

import io.netty.util.ReferenceCounted
import language.experimental.macros
import language.implicitConversions
import scala.reflect.macros._

trait ReferenceCountedSyntax {
  implicit def asRichReferenceCounted[A <: ReferenceCounted](rc: A) =
    new ReferenceCountedSyntax.RichReferenceCounted(rc)
}

object ReferenceCountedSyntax {
  // FIXME: should be a macro
  final class RichReferenceCounted[A <: ReferenceCounted](val rc: A) extends AnyVal {
    def releaseAfter[B](f: A => B): B = macro ReleaseAfterImpl[A, B]
  }

  def ReleaseAfterImpl[A, B](c: blackbox.Context)(f: c.Tree): c.Tree = {
    import c.universe._
    val rc = TermName(c.freshName("rc"))

    q"""{
     val $rc = ${c.prefix.tree}.rc
     try $f($rc) finally $rc.release()
    }"""
  }
}
