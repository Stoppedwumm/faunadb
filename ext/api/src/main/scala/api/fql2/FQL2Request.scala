package fauna.api.fql2

import fauna.codex.json2._
import fauna.model.runtime.fql2.serialization.{
  FQL2Query,
  UndecidedValue,
  ValueFormat
}
import scala.collection.immutable.SeqMap

case class FQL2RequestBody(
  query: FQL2Query,
  arguments: Option[SeqMap[String, UndecidedValue]] = None)

object FQL2RequestBody {
  import FQL2Query._

  implicit val jsonDecoder = {
    implicit object ArgumentsDecoder
        extends JSON.PartialDecoder[SeqMap[String, UndecidedValue]]("Object") {
      override def readObjectStart(stream: JSON.In) = {
        val b = SeqMap.newBuilder[String, UndecidedValue]
        while (!stream.skipObjectEnd) {
          val k = stream.read(JSONParser.ObjectFieldNameSwitch)
          val v = JSON.decode[UndecidedValue](stream)
          b += (k -> v)
        }
        b.result()
      }
    }
    JSON.RecordDecoder[FQL2RequestBody]
  }
}

case class FQL2Request(
  body: FQL2RequestBody,
  format: ValueFormat,
  typecheck: Option[Boolean],
  linearized: Boolean,
  performanceHints: Boolean
)
