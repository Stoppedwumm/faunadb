package fauna.model.test

import fauna.model.AccessProvider

class FQL2StagedAccessProviderSpec extends FQL2StagedSchemaBaseSpec {
  "looks up access provider by active issuer" in {
    updateSchemaOk(
      auth,
      "main.fsl" ->
        """|role foo {}
           |access provider bar {
           |  issuer "https://foo.auth0.com"
           |  jwks_uri "https://foo.auth0.com/.well-known/jwks.json"
           |  role foo
           |}""".stripMargin
    )

    updateSchemaPinOk(
      auth,
      "main.fsl" ->
        """|role foo {}
           |access provider bar {
           |  issuer "https://zzzzzzzzzzz.auth0.com"
           |  jwks_uri "https://zzzzzzzzzzz.auth0.com/.well-known/jwks.json"
           |  role foo
           |}""".stripMargin
    )

    ctx ! AccessProvider.idByIssuer(
      auth.scopeID,
      "https://foo.auth0.com") should not be empty
  }
}
