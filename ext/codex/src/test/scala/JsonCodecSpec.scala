package fauna.codex.test

import fauna.codex.json._
import fauna.codex.json2.{ JSON, JSONParser, PartialJSONSwitch }
import fauna.lang.syntax._
import fauna.prop.BLNS
import java.nio.charset.StandardCharsets
import io.netty.buffer.{ ByteBuf, Unpooled }

class JsonCodecSpec extends Spec {

  case class Widget(id: Int, name: String)

  object Widget {
    implicit val decoder = JsonDecoder.Record("id", "name")(Widget.apply)
    implicit val encoder = JsonEncoder.Record("id", "name")(Widget.unapply)
  }

  case class BunchOfWidgets(id: Int, label: String, widgets: Seq[Widget])

  object BunchOfWidgets {
    implicit val decoder = JsonDecoder.Record("id", "label", "widgets")(BunchOfWidgets.apply)
    implicit val encoder = JsonEncoder.Record("id", "label", "widgets")(BunchOfWidgets.unapply)
  }

  case class ExtensibleWidget(id: Int, data: JSValue)

  object ExtensibleWidget {
    implicit val decoder = JsonDecoder.Record("id", "data")(ExtensibleWidget.apply)
    implicit val encoder = JsonEncoder.Record("id", "data")(ExtensibleWidget.unapply)
  }

  case class WidgetOptions(id: Int, alias: Option[String] = None, age: Option[Int])

  object WidgetOptions {
    implicit val widgetOptionsCodec: JSON.Codec[WidgetOptions] =
      JSON.RecordCodec[WidgetOptions]
  }

  "JSValue" - {
    "encodes & decodes objects" in {
      val data = JSObject("a" -> 1)
      val json = JSON.encode(data).toUTF8String

      json should equal ("""{"a":1}""")

      JSON.parse[JSValue](json.toUTF8Buf) should equal (data)
    }

    "encodes & decodes records" in {
      case class MyRecord(foo: String, bar: Int, baz: Boolean)
      implicit val myRecordCodec = JSON.RecordCodec[MyRecord]

      val data = MyRecord("string value", 123, false)

      val json = JSON.encode(data).toUTF8String
      json should equal("""{"foo":"string value","bar":123,"baz":false}""")

      JSON.parse[MyRecord](json.toUTF8Buf) should equal(data)
    }

    "correctly escapes strings" in {
      val encoded = JSON.encode("\\hello\"world")

      encoded.toUTF8String should equal ("\"\\\\hello\\\"world\"")
      JSON.parse[String](encoded) should equal ("\\hello\"world")
    }

    "correctly encodes & decodes naughty strings" in {
      BLNS foreach { s =>
        val encoded = JSON.encode(s)
        JSON.parse[String](encoded) should equal (s)
      }
    }

    "works" in {
      val data = "[ true ]"
      JsonCodec.decode[Seq[Boolean]](data.getBytes).get should equal (Seq(true))
    }

    "ByteBuf" in {
      val encoded = JsonCodec.encode[ByteBuf]("secret".toUTF8Buf)

      new String(encoded, StandardCharsets.ISO_8859_1) shouldBe "\"c2VjcmV0\""

      JsonCodec.decode[ByteBuf](encoded).get.toUTF8String shouldBe "secret"
    }

    "fails by properly wrapping a jackson exception" in {
      val malformedData = """{ "lol": ["this is broken" }"""
      JsonCodec.decode[Map[String,Seq[String]]](malformedData.getBytes).isFailure should be (true)
    }

    "replace" in {
      val orig = JSObject("foo" -> "bar", "baz" -> JSObject("qux" -> "quux"))
      val a = orig.replace(List("foo"), 1)
      val b = orig.replace(List("baz", "qux"), 1)
      val c = orig.replace(List("no"), 1)
      val d = orig.replace(List("baz", "no"), 1)

      a should equal (JSObject("foo" -> 1, "baz" -> JSObject("qux" -> "quux")))
      b should equal (JSObject("foo" -> "bar", "baz" -> JSObject("qux" -> 1)))
      c should equal (JSObject("foo" -> "bar", "baz" -> JSObject("qux" -> "quux"), "no" -> 1))
      d should equal (JSObject("foo" -> "bar", "baz" -> JSObject("qux" -> "quux", "no" -> 1)))
    }

    "skips values" in {
      val v1 = JSArray(JSObject("foo" -> 1, "bar" -> JSObject("baz" -> "qux")))
      val buf = Unpooled.buffer

      JSON.encode(buf, v1)
      JSON.encode(buf, "foo")

      val stream = JSONParser(buf)
      stream.skip()
      JSON.decode[String](stream) should equal ("foo")
    }

    "delegates EOS" in {
      val numOrEnd = new PartialJSONSwitch[Long] {
        override def readInt(l: Long, s: JSONParser) = l
        override def readEndOfStream(s: JSONParser) = -1
      }

      val buf = Unpooled.buffer
      JSON.encode(buf, 21)
      val stream = JSONParser(buf)

      stream.read(numOrEnd) should equal (21)
      stream.read(numOrEnd) should equal (-1)
    }
  }

  "AdaptedJsonCodec" - {
    "encodes an adapted type" in {
      val widgets = BunchOfWidgets(1, "some widgets", Seq(
        Widget(123, "a widget"),
        Widget(456, "another widget"),
        Widget(789, "a third widget")
      ))

      val json = new String(JsonCodec.encode(widgets))

      json should equal ("""{"id":1,"label":"some widgets","widgets":[""" +
        """{"id":123,"name":"a widget"},""" +
        """{"id":456,"name":"another widget"},""" +
        """{"id":789,"name":"a third widget"}]}""")

      JsonCodec.decode[BunchOfWidgets](json.getBytes).get should equal (widgets)
    }
  }

  "Mixed Codecs" - {
    "encodes a mixed adapted type with JSValue" in {
      val widget = ExtensibleWidget(123, JSObject("thing" -> JS("foo")))
      val json = new String(JsonCodec.encode(widget))

      json should equal ("""{"id":123,"data":{"thing":"foo"}}""")

      JsonCodec.decode[ExtensibleWidget](json.getBytes).get should equal (widget)
    }
  }

  "Option Codec" - {
    "encodes None to null (for required fields) or [no field] (for optional fields)" in {
      val widgetOpts = WidgetOptions(123, None, None)
      val json = JSON.encode(widgetOpts).toUTF8String

      json should equal("""{"id":123,"age":null}""")
    }

    "decodes null to None" in {
      val json = """{"id":123,"alias":null,"age":null}"""
      JSON.parse[WidgetOptions](json.toUTF8Buf) should equal(
        WidgetOptions(123, None, None))
    }

    "decodes [missing optional field] to default value" in {
      val json = """{"id":987,"age":12}"""
      JSON.parse[WidgetOptions](json.toUTF8Buf) should equal(
        WidgetOptions(987, None, Some(12)))
    }

    "encodes & decodes Some(value)" in {
      val widgetOpts =
        WidgetOptions(456, alias = Some("thingamabob"), age = Some(78))
      val json = JSON.encode(widgetOpts).toUTF8String

      json should equal("""{"id":456,"alias":"thingamabob","age":78}""")

      JSON.decode[WidgetOptions](json.toUTF8Buf) should equal(widgetOpts)
    }
  }
}
