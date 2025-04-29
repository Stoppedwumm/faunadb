package fauna.codex.test

import fauna.codex.cbor._
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.prop.BLNS
import io.netty.buffer.{ ByteBuf, Unpooled }
import java.util.UUID
import scala.math.BigInt

class CBORCodecSpec extends Spec {

  case class GenericTuple[T](t1: T, t2: T)

  object GenericTuple {
    implicit def CBORCodec[T: CBOR.Codec] = CBOR.TupleCodec[GenericTuple[T]]
  }

  "CBORCodec" - {
    "infers decoders for generic classes" in {
      // checking compilation only
      implicitly[CBOR.Decoder[GenericTuple[Int]]]
      implicitly[CBOR.Decoder[Vector[Long]]]
      a[Exception] should be thrownBy CBOR.parse[GenericTuple[Int]](null: Array[Byte])
    }

    "infers encoders for generic classes" in {
      // checking compilation only
      implicitly[CBOR.Encoder[GenericTuple[Long]]]
      implicitly[CBOR.Encoder[Vector[Long]]]
      CBOR.encode(GenericTuple(1, 2))
    }

    "infers codecs for generic classes" in {
      implicitly[CBOR.Codec[GenericTuple[Long]]]
      implicitly[CBOR.Codec[Vector[Long]]]
    }

    "builds TupleCodecs" in {
      implicit val c = CBOR.TupleCodec[GenericTuple[Int]]

      val tup = GenericTuple(1, 2)
      CBOR.show(CBOR.encode(tup)) should equal ("[1, 2]")
      CBOR.parse[GenericTuple[Int]](CBOR.encode(tup)) should equal (tup)
    }

    "efficient 1-arity TupleCodecs" in {
      case class Foo(i: Int)
      implicit val c = CBOR.TupleCodec[Foo]

      val f = Foo(2)
      CBOR.show(CBOR.encode(f)) should equal ("2")
      CBOR.parse[Foo](CBOR.encode(f)) should equal (f)
    }

    "builds RecordCodecs" in {
      implicit val c = CBOR.RecordCodec[GenericTuple[Int]]

      val tup = GenericTuple(1, 2)
      CBOR.show(CBOR.encode(tup)) should equal ("""{"t1" : 1, "t2" : 2}""")
      CBOR.parse[GenericTuple[Int]](CBOR.encode(tup)) should equal (tup)
    }

    "builds SumCodecs" in {
      implicit val c = CBOR.SumCodec[Either[Int, String]](
        CBOR.TupleCodec[Left[Int, String]],
        CBOR.TupleCodec[Right[Int, String]])

      CBOR.show(CBOR.encode(Left(2))) should equal ("""6(2)""")
      CBOR.show(CBOR.encode(Right("foo"))) should equal ("""7("foo")""")
      CBOR.parse[Either[Int, String]](CBOR.encode(Left(2))) should equal (Left(2))
    }

    "encodes and decodes singleton types" in {
      val t = CBOR.encode(TrueV).toByteArray
      t should equal (Array[Byte](0xf5.toByte))
      CBORValidator(t).validate()

      val f = CBOR.encode(FalseV).toByteArray
      f should equal (Array[Byte](0xf4.toByte))
      CBORValidator(f).validate()

      val n = CBOR.encode(NilV).toByteArray
      n should equal (Array[Byte](0xf6.toByte))
      CBORValidator(n).validate()

      CBOR.parse[Value](Array[Byte](0xf5.toByte)) should equal (TrueV)
      CBOR.parse[Value](Array[Byte](0xf4.toByte)) should equal (FalseV)
      CBOR.parse[Value](Array[Byte](0xf6.toByte)) should equal (NilV)
    }

    def assertEncodes(i: Long, enc: Array[Byte]) = {
      val iv = IntV(i)
      val b = CBOR.encode(iv).toByteArray
      b should equal (enc)
      CBORValidator(b).validate()
      CBOR.parse[Value](enc) should equal (iv)
    }

    "encodes and decodes unsigned integer types" in {
      assertEncodes(0, Array[Byte](0x00))
      assertEncodes(2, Array[Byte](0x02))
      assertEncodes(23, Array[Byte](0x17))
      assertEncodes(24, Array[Byte](0x18, 0x18))
      assertEncodes(32, Array[Byte](0x18, 0x20))
      assertEncodes(0x100, Array[Byte](0x19, 0x01, 0x00))
      assertEncodes(512, Array[Byte](0x19, 0x02, 0x00))
      assertEncodes(0x10000, Array[Byte](0x1a, 0x00, 0x01, 0x00, 0x00))
      assertEncodes(131072, Array[Byte](0x1a, 0x00, 0x02, 0x00, 0x00))
      assertEncodes(0x100000000L, Array[Byte](0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00))
      assertEncodes(8589934592L, Array[Byte](0x1b, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00))
    }

    "encodes and decodes unsigned integer types with minimal length" in {
      pendingUntilFixed {
        assertEncodes(0xff, Array[Byte](0x18, 0xff.toByte))
        assertEncodes(0xffff, Array[Byte](0x19, 0xff.toByte, 0xff.toByte))
        assertEncodes(0xffffffffL, Array[Byte](0x1a, 0xff.toByte, 0xff.toByte, 0xff.toByte, 0xff.toByte))
      }
    }

    "encodes and decodes negative integer types" in {
      assertEncodes(-1, Array[Byte](0x20))
      assertEncodes(-2, Array[Byte](0x21))
      assertEncodes(-24, Array[Byte](0x37))
      assertEncodes(-25, Array[Byte](0x38, 0x18))
      assertEncodes(-0x101, Array[Byte](0x39, 0x01, 0x00))
      assertEncodes(-512, Array[Byte](0x39, 0x01, 0xff.toByte))
      assertEncodes(-0x10001, Array[Byte](0x3a, 0x00, 0x01, 0x00, 0x00))
      assertEncodes(-131072, Array[Byte](0x3a, 0x00, 0x01, 0xff.toByte, 0xff.toByte))
      assertEncodes(-0x100000001L, Array[Byte](0x3b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00))
      assertEncodes(-8589934592L, Array[Byte](0x3b, 0x00, 0x00, 0x00, 0x01, 0xff.toByte, 0xff.toByte, 0xff.toByte, 0xff.toByte))
    }

    "encodes and decodes negative unsigned integer types with minimal length" in {
      pendingUntilFixed {
        assertEncodes(-0x100, Array[Byte](0x38, 0xff.toByte))
        assertEncodes(-0x10000, Array[Byte](0x39, 0xff.toByte, 0xff.toByte))
        assertEncodes(-0x100000000L, Array[Byte](0x3a, 0xff.toByte, 0xff.toByte, 0xff.toByte, 0xff.toByte))
      }
    }

    "encodes and decodes big integer types" in {
      val big = BigInt("340282366920938463463374607431768211456") // 2^128

      val uint128 = Array[Byte](0xc2.toByte, 0x51) ++ big.toByteArray
      val int128 = Array[Byte](0xc3.toByte, 0x51) ++ big.toByteArray

      val u128 = CBOR.encode(BigIntV(big)).toByteArray
      u128 should equal (uint128)
      CBORValidator(u128).validate()
      CBOR.parse[Value](uint128) should equal (BigIntV(big))

      val i128 = CBOR.encode(BigIntV(-1 - big)).toByteArray
      i128 should equal (int128)
      CBORValidator(i128).validate()
      CBOR.parse[Value](int128) should equal (BigIntV(-1 - big))
    }

    "encodes and decodes fractional integer types" in {
      // 7.0 in IEEE 754 single- and double-precision FP
      val float = Array[Byte](0xfa.toByte, 0x40, 0xe0.toByte, 0x00, 0x00)
      val double = Array[Byte](0xfb.toByte, 0x40, 0x1c, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00)

      CBOR.encode(FloatV(7.0f)).toByteArray should equal (float)
      CBOR.encode(DoubleV(7.0)).toByteArray should equal (double)

      CBOR.parse[Value](float) should equal (FloatV(7.0f))
      CBOR.parse[Value](double) should equal (DoubleV(7.0))
    }

    "encodes and decodes timestamps" in {
      val intTS = Array[Byte](0xc1.toByte, 0x07)
      val arrayTS = Array[Byte](0xc1.toByte, 0x82.toByte, 0x07, 0x07)

      CBOR.encode(Timestamp(7, 0)).toByteArray should equal (intTS)
      CBOR.encode(Timestamp(7, 7)).toByteArray should equal (arrayTS)

      CBOR.parse[Value](intTS) should equal (TimeV(Timestamp(7, 0)))
      CBOR.parse[Value](arrayTS) should equal (TimeV(Timestamp(7, 7)))
    }

    "encodes and decodes UUIDs" in {
      val id = UUID.fromString("6e1a537e-bb82-447f-bc92-343825dfe964") // chosen by random dice roll :>
      val bytes = Array[Byte](
        0x6e, 0x1a, 0x53, 0x7e, 0xbb.toByte, 0x82.toByte, 0x44,
        0x7f, 0xbc.toByte, 0x92.toByte, 0x34, 0x38, 0x25, 0xdf.toByte, 0xe9.toByte, 0x64.toByte)

      // sanity check
      val sanity = Unpooled.wrappedBuffer(bytes)
      (new UUID(sanity.readLong, sanity.readLong)) should equal (id)

      val prefix = Array[Byte](InitialByte.UInt8Tag.toByte, 0x25, 0x50)

      CBOR.encode(id).toByteArray should equal(prefix ++ bytes)
      CBOR.parse[UUID](prefix ++ bytes) should equal(id)
    }

    "encodes and decodes byte strings" in {
      val str = "Hello, world!".toUTF8Buf
      val readable = str.readableBytes

      // various ways to represent the string "Hello, world!" in
      // ASCII-encoded bytes
      val tiny = Array[Byte](0x4d) ++ str.toByteArray
      val uint8 = Array[Byte](0x58, 0x0d) ++ str.toByteArray
      val uint16 = Array[Byte](0x59, 0x00, 0x0d) ++ str.toByteArray
      val uint32 = Array[Byte](0x5a, 0x00, 0x00, 0x00, 0x0d) ++ str.toByteArray

      CBOR.encode(ByteStrV(str)).toByteArray should equal (tiny)

      CBOR.parse[Value](tiny) should equal (ByteStrV(str))
      CBOR.parse[Value](uint8) should equal (ByteStrV(str))
      CBOR.parse[Value](uint16) should equal (ByteStrV(str))
      CBOR.parse[Value](uint32) should equal (ByteStrV(str))

      // doesn't consume ByteBufs
      CBOR.encode(str).toByteArray should equal (tiny)
      str.readableBytes should equal (readable)

      // doesn't consume ByteBuffers
      val buf = str.nioBuffer
      CBOR.encode(buf).toByteArray should equal (tiny)
      buf.remaining should equal (readable)
    }

    def assertEncodesString(len: Int, lenEnc: Array[Byte]) = {
      len % 10 should equal (0)
      val builder = new StringBuilder(len)
      (1 to (len / 10)) foreach { _ => builder ++= "0123456789" }
      val str = builder.toString()
      val enc = lenEnc ++ str.toUTF8Bytes
      val strv = StrV(str)
      CBOR.encode(strv).toByteArray should equal (enc)
      CBOR.parse[Value](enc) should equal (strv)
    }

    "encodes and decodes UTF strings" in {
      val str = "Hello, world!"

      val tiny = Array[Byte](0x6d) ++ str.toUTF8Bytes
      val uint8 = Array[Byte](0x78, 0x0d) ++ str.toUTF8Bytes
      val uint16 = Array[Byte](0x79, 0x00, 0x0d) ++ str.toUTF8Bytes
      val uint32 = Array[Byte](0x7a, 0x00, 0x00, 0x00, 0x0d) ++ str.toUTF8Bytes

      CBOR.encode(StrV(str)).toByteArray should equal (tiny)

      CBOR.parse[Value](tiny) should equal (StrV(str))
      CBOR.parse[Value](uint8) should equal (StrV(str))
      CBOR.parse[Value](uint16) should equal (StrV(str))
      CBOR.parse[Value](uint32) should equal (StrV(str))
    }

    "encodes and decodes UTF strings with minimal-length encoding for string length" in {
      pendingUntilFixed {
        assertEncodesString(160, Array[Byte](0x78, 0xa0.toByte))
        assertEncodesString(260, Array[Byte](0x79, 0x01, 0x04))
        assertEncodesString(32770, Array[Byte](0x79, 0x80.toByte, 0x02))
        assertEncodesString(65530, Array[Byte](0x79, 0xff.toByte, 0xfa.toByte))
        assertEncodesString(65540, Array[Byte](0x7a, 0x00, 0x01, 0x00, 0x04))
      }
    }

    "encodes and decodes naughty strings" in {
      BLNS foreach { s =>
        val bs = CBOR.encode(s).toByteArray
        CBOR.parse[String](bs) should equal (s)
      }
    }

    "encodes and decodes arrays" in {
      val array = Vector(IntV(7), IntV(7), IntV(7),
        IntV(7), ByteStrV("Hello, world!"),
        StrV("Hello, world!"))

      // trust that primitive type tests that this encoding is right
      val encoded = array.foldLeft(Array[Byte](0x86.toByte)) { (ba, i) =>
        ba ++ CBOR.encode(i).toByteArray
      }

      CBOR.encode(ArrayV(array)).toByteArray should equal(encoded)
      CBOR.parse[Value](encoded) should equal (ArrayV(array))
    }

    "encodes and decodes maps" in {
      val map = Vector((StrV("tom"), IntV(7)),
        (StrV("dick"), IntV(7)),
        (StrV("harry"), IntV(7)),
        (StrV("Hello, world!"), ByteStrV("Hello, world!")))

      // as with arrays, trust primitive type tests
      val encoded = map.foldLeft(Array[Byte](0xa4.toByte)) { (ba, i) =>
        ba ++ CBOR.encode(i._1).toByteArray ++ CBOR.encode(i._2).toByteArray
      }

      CBOR.encode(MapV(map)).toByteArray should equal (encoded)
      CBOR.parse[Value](encoded) should equal (MapV(map))
    }

    "encodes and decodes tagged values" in {
      // tag '7', and a selection of data items
      val tag = Array[Byte](0xc7.toByte)
      val taggedNil = tag ++ CBOR.encode(NilV).toByteArray
      val taggedInt = tag ++ CBOR.encode(IntV(7)).toByteArray
      val taggedFloat = tag ++ CBOR.encode(FloatV(7.0f)).toByteArray

      val bytes = ByteStrV("Hello, world!")
      val taggedBytes = tag ++ CBOR.encode(bytes).toByteArray

      val string = StrV("Hello, world!")
      val taggedString = tag ++ CBOR.encode(string).toByteArray

      val array = ArrayV(Vector(IntV(7), IntV(7)))
      val taggedArray = tag ++ CBOR.encode(array).toByteArray

      val map = MapV(Vector((StrV("tom"), IntV(7))))
      val taggedMap = tag ++ CBOR.encode(map).toByteArray

      CBOR.encode(TagV(7, NilV)).toByteArray should equal (taggedNil)
      CBOR.parse[Value](taggedNil) should equal (TagV(7, NilV))

      CBOR.encode(TagV(7, IntV(7))).toByteArray should equal (taggedInt)
      CBOR.parse[Value](taggedInt) should equal (TagV(7, IntV(7)))

      CBOR.encode(TagV(7, FloatV(7.0f))).toByteArray should equal (taggedFloat)
      CBOR.parse[Value](taggedFloat) should equal (TagV(7, FloatV(7.0f)))

      CBOR.encode(TagV(7, bytes)).toByteArray should equal (taggedBytes)
      CBOR.parse[Value](taggedBytes) should equal (TagV(7, bytes))

      CBOR.encode(TagV(7, string)).toByteArray should equal (taggedString)
      CBOR.parse[Value](taggedString) should equal (TagV(7, string))

      CBOR.encode(TagV(7, array)).toByteArray should equal (taggedArray)
      CBOR.parse[Value](taggedArray) should equal (TagV(7, array))

      CBOR.encode(TagV(7, map)).toByteArray should equal (taggedMap)
      CBOR.parse[Value](taggedMap) should equal (TagV(7, map))

      // tags of various sizes
      val payload = CBOR.encode(IntV(7)).toByteArray
      val byte = Array[Byte](0xc6.toByte) ++ payload
      val uint8 = Array[Byte](0xd8.toByte, 0x20) ++ payload
      val uint16 = Array[Byte](0xd9.toByte, 0x02, 0x00) ++ payload
      val uint32 = Array[Byte](0xda.toByte, 0x00, 0x02, 0x00, 0x00) ++ payload
      val uint64 = Array[Byte](0xdb.toByte,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x00) ++ payload

      CBOR.parse[Value](byte) should equal (TagV(6, IntV(7)))
      CBOR.parse[Value](uint8) should equal (TagV(32, IntV(7)))
      CBOR.parse[Value](uint16) should equal (TagV(512, IntV(7)))
      CBOR.parse[Value](uint32) should equal (TagV(131072, IntV(7)))
      CBOR.parse[Value](uint64) should equal (TagV(8589934592L, IntV(7)))

      // Once upon a time, Brandon made a mistake in the CBOR codec
      // which added 6 to all Tag bytes. This tests that we can read
      // the 'old' bytes on disk correctly

      val oldTag = Array[Byte](0xcd.toByte) // used to be tag '7', now interpreted as '13'
      val instanceID = oldTag ++ CBOR.encode(IntV(42)).toByteArray // a hypothetical InstanceIDV

      CBOR.encode(TagV(13, IntV(42))).toByteArray should equal (instanceID)
      CBOR.parse[Value](instanceID) should equal (TagV(13, IntV(42)))
    }

    "encodes and decodes ranges" in {
      val inclusive = 0 to 10 by 2
      val exclusive = 0 until 10 by 2
      CBOR.parse[Range](CBOR.encode(inclusive)) shouldBe inclusive
      CBOR.parse[Range](CBOR.encode(exclusive)) shouldBe exclusive
    }

    "encodes and decodes eithers" in {
      val left = Array[Byte](0xc6.toByte, 0x01)
      val right = Array[Byte](0xc7.toByte, 0x02)

      CBOR.encode[Either[Int, Int]](Left(1)).toByteArray should equal (left)
      CBOR.parse[Either[Int, Int]](left) should equal (Left(1))

      CBOR.encode[Either[Int, Int]](Right(2)).toByteArray should equal (right)
      CBOR.parse[Either[Int, Int]](right) should equal (Right(2))
    }

    "decode consumes input, parse does not" in {
      val buf = Unpooled.buffer
      CBOR.encode(buf, TrueV)
      CBOR.encode(buf, FalseV)

      CBOR.parse[Value](buf) should equal (TrueV)
      CBOR.parse[Value](buf) should equal (TrueV)

      CBOR.decode[Value](buf) should equal (TrueV)

      CBOR.parse[Value](buf) should equal (FalseV)
      CBOR.parse[Value](buf) should equal (FalseV)

      CBOR.decode[Value](buf) should equal (FalseV)

      an[Exception] should be thrownBy CBOR.decode[Value](buf)
    }

    "skips values" in {
      val v1 = ArrayV(Vector(MapV(Vector(
        StrV("foo") -> IntV(7),
        TrueV -> ArrayV(Vector(TagV(4, StrV("bar")))),
        ByteStrV("baz") -> FalseV))))

      val buf = Unpooled.buffer
      CBOR.encode(buf, v1)
      CBOR.encode(buf, IntV(7))

      val stream = CBORParser(buf)
      stream.skip()
      CBOR.decode[Value](stream) should equal (IntV(7))
    }

    "delegates EOS" in {
      val numOrEnd = new PartialCBORSwitch[Long] {
        override def readInt(l: Long, s: CBORParser) = l
        override def readEndOfStream(s: CBORParser) = -1
      }

      val buf = Unpooled.buffer
      CBOR.encode(buf, 21)
      val stream = CBORParser(buf)

      stream.read(numOrEnd) should equal (21)
      stream.read(numOrEnd) should equal (-1)
    }

    "can use raw bytes for naughtiness" in {
      val map = Vector((StrV("tom"), IntV(7)),
        (StrV("dick"), IntV(7)),
        (StrV("harry"), IntV(7)),
        (StrV("Hello, world!"), ByteStrV("Hello, world!")))

      val tup = ArrayV(Vector(MapV(map), IntV(4)))

      // Serialize map, then insert serialized value into tuple
      // and pull the whole object out without anyone being the wiser.
      val encodedMap = CBOR.encode(MapV(map))

      val partial = new CBOR.PartialCodec[(ByteBuf, Int)]("why?") {
          val intCodec = implicitly[CBOR.Codec[Int]]
          override def encode(stream: CBOR.Out, tup: (ByteBuf, Int)): CBOR.Out = {
              val (encoded, i) = tup
              stream.writeArrayStart(2).unsafeWriteBytesRaw(encoded, encoded.readableBytes)
              intCodec.encode(stream, i)
          }
      }

      val encoded = CBOR.encode((encodedMap, 4))(partial)
      CBOR.parse[Value](encoded) should equal (tup)
    }
  }

  "CBOROrdering" - {
    "correctly compares arrays and maps of various lengths" in {
      // the important fact is not the length, but the encoding of the
      // length

      // all encodings of a unary array whose single value is the uint
      // 0.
      val tinyA = Array[Byte]((InitialByte.ArrayMin | 0x01).toByte, 0x00)
      val uint8A = Array[Byte](InitialByte.UInt8Array.toByte, 0x01, 0x00)
      val uint16A = Array[Byte](InitialByte.UInt16Array.toByte, 0x00, 0x01, 0x00)
      val uint32A = Array[Byte](InitialByte.UInt32Array.toByte, 0x00, 0x00, 0x00, 0x01, 0x00)

      Seq(tinyA, uint8A, uint16A, uint32A).combinations(2) foreach { c =>
        val Seq(a, b) = c
        CBOROrdering.compare(Unpooled.wrappedBuffer(a), Unpooled.wrappedBuffer(b)) should equal (0)
      }

      // same for maps, a single pair (0, 0)
      val tinyM = Array[Byte]((InitialByte.MapMin | 0x01).toByte, 0x00, 0x00)
      val uint8M = Array[Byte](InitialByte.UInt8Map.toByte, 0x01, 0x00, 0x00)
      val uint16M = Array[Byte](InitialByte.UInt16Map.toByte, 0x00, 0x01, 0x00, 0x00)
      val uint32M = Array[Byte](InitialByte.UInt32Map.toByte, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00)

      Seq(tinyM, uint8M, uint16M, uint32M).combinations(2) foreach { c =>
        val Seq(a, b) = c
        CBOROrdering.compare(Unpooled.wrappedBuffer(a), Unpooled.wrappedBuffer(b)) should equal (0)
      }
    }

    "correctly compares bytestrings" in {
      CBOROrdering.compare(CBOR.encode(ByteStrV("xVEgHUeIEjwFbBsuTyCvgiIHmKeqaZ")), CBOR.encode(ByteStrV("xVEgHUeIEjwFbBsuTyCvgiIHmKeqaZazUIyxcfsIOMwZh"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(ByteStrV("AAAAbbabaBbABA")), CBOR.encode(ByteStrV("BB"))) < 0 should equal (true)
    }

    "correctly compares negative to non-negative doubles" in {
      CBOROrdering.compare(CBOR.encode(DoubleV(-1)), CBOR.encode(DoubleV(0))) < 0 should equal (true)
    }

    "can read negative integers between -1 and -24" in {
      CBOROrdering.compare(CBOR.encode(IntV(-3)), CBOR.encode(IntV(-2))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(IntV(-25)), CBOR.encode(IntV(-24))) < 0 should equal (true)
    }

    "reversal applies to only one slot" in {
      val a = CBOR.encode(ArrayV(Vector(TagV(CBOR.ReverseTag, TrueV), IntV(1))))
      val b = CBOR.encode(ArrayV(Vector(TagV(CBOR.ReverseTag, TrueV), IntV(2))))
      CBOROrdering.compare(a, b) should equal (-1)

      val c = CBOR.encode(ArrayV(Vector(TrueV, IntV(1))))
      val d = CBOR.encode(ArrayV(Vector(TrueV, IntV(2))))
      CBOROrdering.compare(c, d) should equal (-1)
    }

    "correctly compares naughty strings" in {
      BLNS foreach { s =>
        CBOROrdering.compare(CBOR.encode(StrV(s)), CBOR.encode(StrV("a" + s))) should not equal (0)
      }
    }

    "correctly compares strings" in {
      CBOROrdering.compare(CBOR.encode(StrV("dddd")), CBOR.encode(StrV("Ddddkvew"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("baGe")), CBOR.encode(StrV("BaGEl"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("")), CBOR.encode(StrV(""))) == 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("uoeuo")), CBOR.encode(StrV(""))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("OENstas=ns")), CBOR.encode(StrV("zz:SSQT"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("FALL")), CBOR.encode(StrV("fall"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("faLLLL")), CBOR.encode(StrV("fall"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("fall")), CBOR.encode(StrV("FALL"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("AAAAbbabaBbABA")), CBOR.encode(StrV("BB"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("baGel")), CBOR.encode(StrV("BagEL"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("baGel")), CBOR.encode(StrV("BagE"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("baGe")), CBOR.encode(StrV("BaGEl"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("biz")), CBOR.encode(StrV("bizness"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("bizness")), CBOR.encode(StrV("biz"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("biza")), CBOR.encode(StrV("bizness"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("bizness")), CBOR.encode(StrV("biza"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("")), CBOR.encode(StrV("ueoeu"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("֑荟Ʞꬫ텨佌ڍòힽ垃㘯䌍拨㫆")), CBOR.encode(StrV("֑荟Ʞꬫ텨佌ڍòힽ垃㘯䌍拨㫆"))) == 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("⺞")), CBOR.encode(StrV("֑荟Ʞꬫ텨佌ڍòힽ垃㘯䌍拨㫆"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("〯揧蟝鵽䟰圦ࠍ嫂份嚒룣˵ꚬ濔Ⱋ")), CBOR.encode(StrV("ᳵ"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("箛")), CBOR.encode(StrV("༿"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("༿")), CBOR.encode(StrV(""))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("뢷")), CBOR.encode(StrV("ࣨ끙⋥䋉"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("ﹲꏐ轙띖᜞밙Β벍빓萄Ᵹ")), CBOR.encode(StrV("乃"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("乃")), CBOR.encode(StrV("ﹲꏐ轙띖᜞밙Β벍빓萄Ᵹ"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("⃟쥪ツꆬ鎦쫘≲᛬")), CBOR.encode(StrV("춶"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("䮶")), CBOR.encode(StrV("⃚踉摠嗿ኻᒲ辻ꘈ⇺凍髦듬蔴젙耨窅댖"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("틇")), CBOR.encode(StrV("‍튯"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("‍튯")), CBOR.encode(StrV("‍틇"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("퐢")), CBOR.encode(StrV("‍‍튯"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("乃")), CBOR.encode(StrV("‍ﹲꏐ轙띖᜞밙Β벍빓萄Ᵹ"))) > 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("乃乃")), CBOR.encode(StrV("乃乃轙띖"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("ﹲꏐ轙띖᜞밙Β벍빓萄Ᵹ")), CBOR.encode(StrV("‍乃"))) < 0 should equal (true)
      CBOROrdering.compare(CBOR.encode(StrV("")), CBOR.encode(StrV("‍‍튯"))) > 0 should equal (true)
    }

    "compare numbers" in {
      CBOROrdering.compare(CBOR.encode(IntV(0)), CBOR.encode(IntV(0))) should equal (0)
      CBOROrdering.compare(CBOR.encode(DoubleV(0)), CBOR.encode(IntV(0))) should equal (0)
      CBOROrdering.compare(CBOR.encode(IntV(0)), CBOR.encode(DoubleV(0))) should equal (0)
      CBOROrdering.compare(CBOR.encode(DoubleV(0)), CBOR.encode(DoubleV(0))) should equal (0)

      CBOROrdering.compare(CBOR.encode(DoubleV(1.1)), CBOR.encode(IntV(0))) should be > 0
      CBOROrdering.compare(CBOR.encode(IntV(2)), CBOR.encode(DoubleV(1.1))) should be > 0

      CBOROrdering.compare(CBOR.encode(DoubleV(1.1)), CBOR.encode(IntV(2))) should be < 0
      CBOROrdering.compare(CBOR.encode(IntV(1)), CBOR.encode(DoubleV(1.1))) should be < 0

      CBOROrdering.compare(CBOR.encode(DoubleV(-0.0)), CBOR.encode(DoubleV(+0.0))) should be < 0
      CBOROrdering.compare(CBOR.encode(DoubleV(+0.0)), CBOR.encode(DoubleV(-0.0))) should be > 0
    }

    "compare mixed types" in {
      //number < ascii < utf8 < array < map < false < true < null
      CBOROrdering.compare(CBOR.encode(2.0f), CBOR.encode(3)) should be < 0
      CBOROrdering.compare(CBOR.encode(2.0d), CBOR.encode(3)) should be < 0

      CBOROrdering.compare(CBOR.encode[Int](2), CBOR.encode("ascii".getBytes)) should be < 0
      CBOROrdering.compare(CBOR.encode[Float](2.0f), CBOR.encode("ascii".getBytes)) should be < 0
      CBOROrdering.compare(CBOR.encode[Double](2.0d), CBOR.encode("ascii".getBytes)) should be < 0

      CBOROrdering.compare(CBOR.encode("ascii".getBytes), CBOR.encode("utf8")) should be < 0

      CBOROrdering.compare(CBOR.encode("utf8"), CBOR.encode(Vector(1, 2, 3))) should be < 0
      CBOROrdering.compare(CBOR.encode(Vector(1, 2, 3)), CBOR.encode(Map("x" -> 10))) should be < 0
      CBOROrdering.compare(CBOR.encode(Map("x" -> 10)), CBOR.encode(false)) should be < 0
      CBOROrdering.compare(CBOR.encode(false), CBOR.encode(true)) should be < 0
      CBOROrdering.compare(CBOR.encode(true), CBOR.encode(NilV)) should be < 0

      //null > true > false > map > array > utf8 > ascii > number
      CBOROrdering.compare(CBOR.encode(NilV), CBOR.encode(true)) should be > 0
      CBOROrdering.compare(CBOR.encode(true), CBOR.encode(false)) should be > 0

      CBOROrdering.compare(CBOR.encode(false), CBOR.encode(Map("x" -> 10))) should be > 0
      CBOROrdering.compare(CBOR.encode(Map("x" -> 10)), CBOR.encode(Vector(1, 2, 3))) should be > 0
      CBOROrdering.compare(CBOR.encode(Vector(1, 2, 3)), CBOR.encode("utf8")) should be > 0

      CBOROrdering.compare(CBOR.encode("utf8"), CBOR.encode("ascii".getBytes)) should be > 0

      CBOROrdering.compare(CBOR.encode("ascii".getBytes), CBOR.encode[Int](2)) should be > 0
      CBOROrdering.compare(CBOR.encode("ascii".getBytes), CBOR.encode[Float](2.0f)) should be > 0
      CBOROrdering.compare(CBOR.encode("ascii".getBytes), CBOR.encode[Double](2.0d)) should be > 0

      CBOROrdering.compare(CBOR.encode(3), CBOR.encode(2.0f)) should be > 0
      CBOROrdering.compare(CBOR.encode(3), CBOR.encode(2.0d)) should be > 0
    }


    "resets UTF-8 sequence state" in {
      // These strings are a minimized representation of some user data which
      // reproduces index corruption in LSE-XXX. During comparison, they cause
      // the collator to reset to the first character of the string for
      // case-insensitive comparison.
      val a = Array[Int](0x78, 0x55, 0x4E, 0x69, 0x67, 0x68, 0x74, 0x2D, 0x74, 0x69, 0x6D, 0x65, 0x29, 0x20, 0x5C, 0x6E, 0x5C, 0x6E, 0x22, 0x7D, 0x2C, 0x7B, 0x22, 0x69, 0x64, 0x22, 0x3A, 0x22, 0x33, 0x63, 0x35, 0x62, 0x33, 0x66, 0x61, 0x63, 0x2D, 0x37, 0x30, 0x34, 0x63, 0x2D, 0x36, 0x66, 0x37, 0x63, 0x2D, 0x62, 0x30, 0x66, 0x61, 0x2D, 0x35, 0x65, 0x62, 0x34, 0x61, 0x61, 0x35, 0x37, 0x66, 0x62, 0x31, 0x30, 0x22, 0x2C, 0x22, 0x61, 0x6E, 0x73, 0x77, 0x65, 0x72, 0x22, 0x3A, 0x22, 0xF0, 0x9F, 0x8F, 0x8C, 0xF0, 0x9F, 0x8F, 0xBC, 0xE2, 0x80, 0x8D) map { _.toByte }

val b = Array[Int](0x78, 0x55, 0x6E, 0x69, 0x67, 0x68, 0x74, 0x2D, 0x74, 0x69, 0x6D, 0x65, 0x29, 0x20, 0x5C, 0x6E, 0x5C, 0x6E, 0x22, 0x7D, 0x2C, 0x7B, 0x22, 0x69, 0x64, 0x22, 0x3A, 0x22, 0x33, 0x63, 0x35, 0x62, 0x33, 0x66, 0x61, 0x63, 0x2D, 0x37, 0x30, 0x34, 0x63, 0x2D, 0x36, 0x66, 0x37, 0x63, 0x2D, 0x62, 0x30, 0x66, 0x61, 0x2D, 0x35, 0x65, 0x62, 0x34, 0x61, 0x61, 0x35, 0x37, 0x66, 0x62, 0x31, 0x30, 0x22, 0x2C, 0x22, 0x61, 0x6E, 0x73, 0x77, 0x65, 0x72, 0x22, 0x3A, 0x22, 0xF0, 0x9F, 0x8F, 0x8C, 0xF0, 0x9F, 0x8F, 0xBC, 0xE2, 0x80, 0x8D) map { _.toByte }

      val cmp = CBOROrdering.compare(Unpooled.wrappedBuffer(a), Unpooled.wrappedBuffer(b))
      CBOROrdering.compare(Unpooled.wrappedBuffer(b), Unpooled.wrappedBuffer(a)) should be (-cmp)
    }
  }
}
