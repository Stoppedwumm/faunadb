package fauna.trace

import fauna.codex.json._

object Kind {
  val Default = Internal

  implicit object JSEncoder extends JsonEncoder[Kind] {
    def encodeTo(stream: JsonGenerator, value: Kind): Unit = {
      val str = value match {
        case Client => "client"
        case Server => "server"
        case Producer => "producer"
        case Consumer => "consumer"
        case Internal => "internal"
      }

      stream.put(str)
    }
  }
}

/**
  * Kind describes the relationship between a Span, its parents, and
  * its children in a trace. It describes two independent properties,
  * as follows:
  *
  * Client: Synchronous, Remote Outbound
  * Server: Synchronous, Remote Inbound
  * Producer: Asynchronous, Remote Outbound
  * Consumer: Asynchronous, Remote Inbound
  *
  * As a rule, a Span should fulfill only a single purpose. For
  * example, a Server span should not be used to directly parent
  * another remote Span.
  */
sealed abstract class Kind
case object Client extends Kind
case object Server extends Kind
case object Producer extends Kind
case object Consumer extends Kind
case object Internal extends Kind
