package fauna.flags.test

import fauna.atoms._
import fauna.codex.cbor._
import fauna.flags._
import fauna.prop.Prop

class PropertiesSpec extends Spec {
  once("encodes") {
    for {
      id <- Prop.long
      long <- Prop.long
      double <- Prop.double
      bool <- Prop.boolean
      string <- Prop.alphaNumString()
    } {
      val codec = AccountProperties.PropertiesVecEncoder
      val props: Map[String, Value] = Map(
        long.toString -> long,
        double.toString -> double,
        bool.toString -> bool,
        string -> string)

      val request = Map(id -> Map("properties" -> props))

      val a = Vector(AccountProperties(AccountID(id), props))

      val bytes = CBOR.encode(a)(codec)
      bytes should equal (CBOR.encode(request))
    }
  }
}
