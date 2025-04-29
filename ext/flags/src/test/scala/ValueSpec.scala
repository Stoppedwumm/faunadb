package fauna.flags.test

import fauna.codex.cbor._
import fauna.flags._
import fauna.prop.Prop

class ValueSpec extends Spec {
  prop("strings") {
    for {
      string <- Prop.alphaNumString()
    } {
      val plain = CBOR.encode(string)
      val ctor = CBOR.encode(StringValue(string))

      plain should equal (ctor)
      CBOR.decode[Value](plain) should equal (StringValue(string))
      CBOR.decode[String](ctor) should equal (string)
    }
  }

  prop("longs") {
    for {
      long <- Prop.long
    } {
      val plain = CBOR.encode(long)
      val ctor = CBOR.encode(LongValue(long))

      plain should equal (ctor)
      CBOR.decode[Value](plain) should equal (LongValue(long))
      CBOR.decode[Long](ctor) should equal (long)
    }
  }

  prop("doubles") {
    for {
      double <- Prop.double
    } {
      val plain = CBOR.encode(double)
      val ctor = CBOR.encode(DoubleValue(double))

      plain should equal (ctor)
      CBOR.decode[Value](plain) should equal (DoubleValue(double))
      CBOR.decode[Double](ctor) should equal (double)
    }
  }

  prop("booleans") {
    for {
      boolean <- Prop.boolean
    } {
      val plain = CBOR.encode(boolean)
      val ctor = CBOR.encode(BooleanValue(boolean))

      plain should equal (ctor)
      CBOR.decode[Value](plain) should equal (BooleanValue(boolean))
      CBOR.decode[Boolean](ctor) should equal (boolean)
    }
  }
}
