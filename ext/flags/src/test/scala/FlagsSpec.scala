package fauna.flags.test

import fauna.atoms._
import fauna.codex.cbor._
import fauna.flags._
import fauna.prop.Prop

class FlagsSpec extends Spec {
  once("decodes") {
    for {
      id <- Prop.long
      long <- Prop.long
      double <- Prop.double
      bool <- Prop.boolean
      string <- Prop.alphaNumString()
    } {
      val codec = AccountFlags.FlagsVecDecoder
      val flags: Map[String, Value] = Map(
        long.toString -> long,
        double.toString -> double,
        bool.toString -> bool,
        string -> string)

      val response = Map(id -> Map("flags" -> flags))

      val bytes = CBOR.encode(response)
      CBOR.decode(bytes)(codec) should equal(
        Vector(AccountFlags(AccountID(id), flags)))
    }
  }

  once("get") {
    for {
      id <- Prop.long
    } {
      val present = AccountFlags(AccountID(id), Map(RunQueries.key -> false))
      val missing = AccountFlags(AccountID(id), Map.empty)
      val failure = AccountFlags(AccountID(id), Map(RunQueries.key -> id))

      present.get(RunQueries) should equal(false)
      missing.get(RunQueries) should equal(RunQueries.default.value)
      failure.get(RunQueries) should equal(RunQueries.default.value)
    }
  }
}
