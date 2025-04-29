package fauna.stats.test

import fauna.codex.json._
import fauna.codex.json2.{ JSON, JSONWriter }
import fauna.stats._
import io.netty.buffer._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class StatsRequestBufferSpec extends AnyFreeSpec with Matchers {
  "outputs JSON" in {
    val buf = new StatsRequestBuffer

    buf.set("pi", 3.14)
    buf.set("name", "alice")

    buf.count("lives", 9)

    buf.incr("enters")
    buf.incr("enters")
    buf.decr("enters")

    buf.timing("milliseconds", 42)
    buf.distribution("latency", 2)

    val bytes = ByteBufAllocator.DEFAULT.buffer
    try {
      val out = JSONWriter(bytes)

      out.writeObjectStart()
      out.writeObjectField(
        JSON.Escaped("stats"), {
          out.writeDelimiter()
          buf.toJson(out)
        })
      out.writeObjectEnd()

      val json = JS.parse(bytes)

      (json / "stats" / "pi") should equal(JSLong(3))
      (json / "stats" / "name") should equal(JSString("alice"))
      (json / "stats" / "lives") should equal(JSLong(9))
      (json / "stats" / "enters") should equal(JSLong(1))
      (json / "stats" / "milliseconds") should equal(JSLong(42))
      (json / "stats" / "latency") should equal(JSLong(2))
    } finally {
      bytes.release()
    }
  }
}
