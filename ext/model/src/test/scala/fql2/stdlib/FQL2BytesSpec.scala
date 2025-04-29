package fauna.model.test

import fauna.model.runtime.fql2.stdlib
import fauna.repo.values.Value
import org.scalactic.source.Position
import scala.collection.immutable.ArraySeq

class FQL2BytesSpec extends FQL2StdlibHelperSpec("Bytes", stdlib.BytesPrototype) {
  val auth = newDB

  def testStaticSig(sig: String*)(implicit pos: Position) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(stdlib.BytesCompanion, test.function) shouldBe sig
    }

  "fromBase64" - {
    testStaticSig("fromBase64(encoded: String) => Bytes")

    "works" in {
      checkOk(auth, "Bytes.fromBase64('AQID')", Value.Bytes(ArraySeq[Byte](1, 2, 3)))
    }
  }

  "apply" - {
    "works" in {
      checkOk(auth, "Bytes('AQID')", Value.Bytes(ArraySeq[Byte](1, 2, 3)))
    }
  }

  "toBase64" - {
    testSig("toBase64() => String")

    "works" in {
      checkOk(auth, "Bytes.fromBase64('AQID').toBase64()", Value.Str("AQID"))
    }
  }

  "toString" - {
    testSig("toString() => String")

    "works" in {
      checkOk(auth, "Bytes.fromBase64('AQID').toString()", Value.Str("AQID"))
    }
  }
}
