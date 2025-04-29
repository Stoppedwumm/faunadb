package fauna.codex.cbor

// See RFC 7049 Section 2.1
object MajorType {
  final val PositiveInt = 0
  final val NegativeInt = 1
  final val ByteString = 2
  final val UTF8String = 3
  final val Array = 4
  final val Map = 5
  final val Tag = 6
  final val Misc = 7
}

object TypeLabels {
  // 'byte' isn't in the CBOR spec; aliases uint8 to j.l.Byte
  final val ByteLabel = "Byte"

  final val IntLabel = "Int"
  final val ByteStringLabel = "Byte String"
  final val UTF8StringLabel = "UTF8 String"
  final val ArrayLabel = "Array"
  final val MapLabel = "Map"
  final val BigNumLabel = "BigNum"
  final val TimestampLabel = "Timestamp"
  final val TagLabel = "Tag"
  final val BooleanLabel = "Boolean"
  final val NilLabel = "Nil"
  final val FloatLabel = "Float"
  final val DoubleLabel = "Double"
  final val MiscLabel = "Misc"
}

object TypeInfos {
  final val FalseInfo = 20
  final val TrueInfo = 21
  final val NilInfo = 22
  final val FloatInfo = 26
  final val DoubleInfo = 27

  final val EpochTimestampTag = 1
  final val PositiveBigNumTag = 2
  final val NegativeBigNumTag = 3
}

// Directly-encoded initial byte values, for ease of use in encoders
object InitialByte {
  // NB. Bytes below 0x18 are directly-encoded uint8s.

  // WARNING: The JVM has no unsigned number types. Caveat computatrum.
  final val UIntMin = 0x00
  final val UInt8 = 0x18
  final val UInt16 = 0x19
  final val UInt32 = 0x1a
  final val UInt64 = 0x1b

  final val NegIntMin = 0x20
  final val NegIntMax = 0x37
  final val NegInt8 = 0x38
  final val NegInt16 = 0x39
  final val NegInt32 = 0x3a
  final val NegInt64 = 0x3b

  final val ByteStrMin = 0x40
  final val ByteStrMax = 0x57
  final val UInt8ByteStr = 0x58
  final val UInt16ByteStr = 0x59
  final val UInt32ByteStr = 0x5a
  final val UInt64ByteStr = 0x5b
  final val VarByteStr = 0x5f

  final val StrMin = 0x60
  final val StrMax = 0x77
  final val UInt8Str = 0x78
  final val UInt16Str = 0x79
  final val UInt32Str = 0x7a
  final val UInt64Str = 0x7b
  final val VarStr = 0x7f

  final val ArrayMin = 0x80
  final val ArrayMax = 0x97
  final val UInt8Array = 0x98
  final val UInt16Array = 0x99
  final val UInt32Array = 0x9a
  final val UInt64Array = 0x9b
  final val VarArray = 0x9f

  final val MapMin = 0xa0
  final val MapMax = 0xb7
  final val UInt8Map = 0xb8
  final val UInt16Map = 0xb9
  final val UInt32Map = 0xba
  final val UInt64Map = 0xbb
  final val VarMap = 0xbf

  final val TagMin = 0xc0
  final val EpochTimestampTag = 0xc1
  final val PositiveBigNumTag = 0xc2
  final val NegativeBigNumTag = 0xc3
  final val TagMax = 0xd4

  final val UInt8Tag = 0xd8
  final val UInt16Tag = 0xd9
  final val UInt32Tag = 0xda
  final val UInt64Tag = 0xdb

  final val False = 0xf4
  final val True = 0xf5
  final val Nil = 0xf6
  final val Float = 0xfa
  final val Double = 0xfb
}
