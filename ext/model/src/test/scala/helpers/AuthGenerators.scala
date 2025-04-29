package fauna.model.test

import fauna.net.security.JWK
import fauna.prop.Prop

trait AuthGenerators {

  def aJWK: Prop[JWK] =
    for {
      keyPair <- Prop.aRSAKeyPair()
      kid <- Prop.hexString()
    } yield { JWK.rsa(kid, keyPair) }
}
