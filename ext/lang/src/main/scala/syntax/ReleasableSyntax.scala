package fauna.lang

import io.netty.util.ReferenceCounted
import scala.util.Using

trait ReleasableSyntax {
  /**
    * Allows Netty reference counted objects to be released
    * automatically with Using, like so:
    *
    *  Using.resource(buf.retain) { buf =>
    *    // do something with buf
    *  } // buf.release() is called here
    *
    */
  implicit object ReleasableReference extends Using.Releasable[ReferenceCounted] {
    def release(ref: ReferenceCounted): Unit =
      ref.release()
  }
}
