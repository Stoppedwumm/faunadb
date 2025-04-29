package fauna.storage.test

import fauna.atoms.{ APIVersion, CollectionID, DocID, SubID }
import fauna.codex.cbor._
import fauna.lang.Timestamp
import fauna.lang.syntax._
import fauna.storage.ir._
import io.netty.buffer.Unpooled
import java.util.UUID
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class IRValueSpec extends AnyFreeSpec with Matchers {
  "MapV" - {
    "get returns a value" in {
      MapV("foo" -> "bar").get(List("foo")) should equal (Some(StringV("bar")))
      MapV("foo" -> "bar").get(List("foo", "baz")) should equal (None)

      MapV("foo" -> NullV).get(List("foo")) should equal (Some(NullV))
      MapV("foo" -> NullV).get(List("foo", "baz")) should equal (Some(NullV))

      MapV("foo" -> "bar").get(List("baz")) should equal (None)
    }

    "rename works" in {
      val t0 = MapV("foo" -> MapV("bar" -> "leaf")).rename(List("foo"), "newFoo")
      t0 should equal (MapV("newFoo" -> MapV("bar" -> "leaf")))

      val t1 = MapV("foo" -> MapV("bar" -> "leaf")).rename(List("foo", "bar"), "newBar")
      t1 should equal (MapV("foo" -> MapV("newBar" -> "leaf")))

      val t2 = MapV("foo" -> ArrayV()).rename(List("foo"), "newFoo")
      t2 should equal (MapV("newFoo" -> ArrayV()))

      val t3 = MapV("foo" -> NullV).rename(List("foo"), "newFoo")
      t3 should equal (MapV("newFoo" -> NullV))

      val m4 = MapV(
        "foo0" -> "String",
        "foo1" -> ArrayV(),
        "foo2" -> MapV())

      val m4Expected0 = MapV(
        "newFoo" -> "String",
        "foo1"   -> ArrayV(),
        "foo2"   -> MapV())

      val m4Expected1 = MapV(
        "foo0"   -> "String",
        "newFoo" -> ArrayV(),
        "foo2"   -> MapV())

      val m4Expected2 = MapV(
        "foo0"   -> "String",
        "foo1"   -> ArrayV(),
        "newFoo" -> MapV())

      m4.rename(List("foo0"), "newFoo") should equal(m4Expected0)
      m4.rename(List("foo1"), "newFoo") should equal(m4Expected1)
      m4.rename(List("foo2"), "newFoo") should equal(m4Expected2)

      MapV("foo" -> ArrayV()).rename(List("foo", "bar"), "newBar") should equal (MapV("foo" -> ArrayV()))
      MapV("foo" -> NullV).rename(List("foo", "bar"), "newBar") should equal (MapV("foo" -> NullV))
      MapV("foo" -> QueryV.Null).rename(List("foo", "bar"), "newBar") should equal (MapV("foo" -> QueryV.Null))
    }
  }

  "QueryV" - {
    val queryTagByte = (InitialByte.TagMin | CBOR.QueryTag).toByte
    val expr = MapV("lambda" -> "x", "expr" -> MapV("var" -> "x"))

    def rawEncodedQuery(inner: IRValue) =
      Unpooled.copiedBuffer(Unpooled.wrappedBuffer(Array(queryTagByte)), CBOR.encode(inner))

    "decodes malformed tagged queries as QueryV.Null" in {
      CBOR.decode[IRValue](rawEncodedQuery(LongV(42))) should equal (QueryV.Null)
    }

    "decodes queries with no version tag as v2.12" in {
      val expected = QueryV(APIVersion.LambdaDefaultVersion, expr)
      CBOR.decode[IRValue](rawEncodedQuery(expr)) should equal (expected)
    }

    "round-trips through CBOR, preserving the API version" in {
      APIVersion.Versions foreach { vers =>
        val q = QueryV(vers, expr)
        val expected = QueryV(vers max APIVersion.LambdaDefaultVersion, expr)

        CBOR.decode[IRValue](CBOR.encode(q)) should equal(expected)
      }
    }
  }

  "ScalarV serialized size estimates" - {
    def serializedSize(s: IRValue): Long = CBOR.encode(s).readableBytes()

    "all types" in {
      Seq(
        TransactionTimeV(isMicros = true),
        LongV(99),
        DoubleV(99.99),
        BooleanV(true),
        NullV,
        StringV("hello"),
        BytesV("hello".toUTF8Buf),
        DocIDV(DocID(SubID(0), CollectionID(0))),
        TimeV(Timestamp.Epoch),
        DateV(LocalDate.EPOCH),
        UUIDV(UUID.randomUUID()),
        ArrayV((1 to 1000).map(LongV(_)).toVector),
        MapV((1 to 1000).map(i => i.toString -> LongV(i)).toList)
      ) foreach { s =>
        (IRValue.serializedSizeLowerBound(s) <= serializedSize(s)) should equal(true)
      }
    }
  }

  "Partial codec" - {
    def encodeAndRead(ir: IRValue, path: List[String]): Option[IRValue] =
      CBORParser(CBOR.encode(ir)).read(new PartialIRCodec(path))

    "scalars and arrays" in {
      encodeAndRead(LongV(0), List("a")) shouldBe Some(LongV(0))
      encodeAndRead(DoubleV(1.2), List("a")) shouldBe Some(DoubleV(1.2))
      encodeAndRead(StringV("hello"), List("a")) shouldBe Some(StringV("hello"))

      val arr = ArrayV(LongV(1), DoubleV(2.1), ArrayV(LongV(2)))
      encodeAndRead(arr, List("a")) shouldBe Some(arr)
    }

    "matches" in {
      val map = MapV((1 to 4) map { n => (n.toString, LongV(n)) }: _*)
      (1 to 4) foreach { n =>
        encodeAndRead(map, List(n.toString)) shouldBe Some(LongV(n))
      }

      val map2 = MapV(
        ("a", MapV(
          ("b", LongV(10)),
          ("c", LongV(11))
        )),
        ("x", MapV(
          ("b", LongV(20)),
          ("c", LongV(21))
        ))
      )
      encodeAndRead(map2, List("a")) shouldBe Some(MapV(("b", LongV(10)), ("c", LongV(11))))
      encodeAndRead(map2, List("a", "b")) shouldBe Some(LongV(10))
      encodeAndRead(map2, List("x", "c")) shouldBe Some(LongV(21))
    }

    "doesn't match" in {
      encodeAndRead(MapV(("a", LongV(0))), List("b")) shouldBe None
      encodeAndRead(MapV(("a", MapV(("b", LongV(0))))), List("b")) shouldBe None
      encodeAndRead(MapV(("a", MapV(("b", LongV(0))))), List("b", "a")) shouldBe None
      encodeAndRead(MapV(("a", MapV(("b", LongV(0))))), List("a", "a")) shouldBe None
    }

    "combines with arrays" in {
      val arrRoot = ArrayV(MapV(("a", LongV(1))))
      encodeAndRead(arrRoot, List("a")) shouldBe Some(arrRoot)
      encodeAndRead(arrRoot, List("b")) shouldBe Some(arrRoot)

      val mapRoot = MapV(("a", ArrayV(MapV(("b", LongV(2))))))
      encodeAndRead(mapRoot, List("a")) shouldBe Some(ArrayV(MapV(("b", LongV(2)))))
      encodeAndRead(mapRoot, List("b")) shouldBe None
      encodeAndRead(mapRoot, List("a", "b")) shouldBe Some(ArrayV(MapV(("b", LongV(2)))))
      encodeAndRead(mapRoot, List("a", "c")) shouldBe Some(ArrayV(MapV(("b", LongV(2)))))
    }
  }
}
