package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth, JWTToken }
import fauna.codex.json.JSObject
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.Database
import fauna.net.security._
import fauna.repo.values.Value
import java.time.Instant

class FQL2AuthSpec extends FQL2Spec with AuthGenerators {

  def login(auth: Auth, doc: Value.Doc, coll: String, passwd: String) = {
    val token = evalOk(
      auth,
      s"""|let doc = $coll.byId('${doc.id.subID.toLong}')!
          |let cred = Credentials.byDocument(doc)!
          |cred.login('$passwd')""".stripMargin
    )

    (getDocFields(auth, token) / "secret").as[String]
  }

  def mkCredentials(auth: Auth, doc: Value.Doc, coll: String, passwd: String) = {
    evalOk(
      auth,
      s"""|Credentials.create({
          |  document: $coll.byId('${doc.id.subID.toLong}'),
          |  password: '$passwd'
          |})""".stripMargin
    )
  }

  "FQL2AuthSpec" - {

    "roles" - {
      "works" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Collection.create({ name: 'Books' })")
        evalOk(admin, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Function.create({
             |  name: "CreateBooksFunc",
             |  body: "() => Books.create({})"
             |})
             |
             |Function.create({
             |  name: "CreateUsersFunc",
             |  body: "() => Users.create({})"
             |})
             |""".stripMargin
        )

        evalOk(
          admin,
          """|Role.create({
             |  name: "CreateBooks",
             |  privileges: [{
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }, {
             |    resource: "CreateBooksFunc",
             |    actions: {
             |      call: true
             |    }
             |  }]
             |})
             |
             |Role.create({
             |  name: "CreateUsers",
             |  privileges: [{
             |    resource: "Users",
             |    actions: {
             |      create: true
             |    }
             |  }, {
             |    resource: "CreateUsersFunc",
             |    actions: {
             |      call: true
             |    }
             |  }]
             |})
             |""".stripMargin
        )

        val createBooksKey = mkKey(admin, "CreateBooks")
        val createUsersKey = mkKey(admin, "CreateUsers")

        evalOk(createBooksKey, "Books.create({})")
        evalOk(createBooksKey, "CreateBooksFunc()")
        evalErr(createBooksKey, "Users.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
        evalErr(createBooksKey, "CreateUsersFunc()") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(createUsersKey, "Books.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
        evalErr(createUsersKey, "CreateBooksFunc()") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
        evalOk(createUsersKey, "Users.create({})")
        evalOk(createUsersKey, "CreateUsersFunc()")
      }

      "membership resource can be a collection name" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Collection.create({ name: 'Books' })")
        evalOk(admin, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "AdminUsers",
             |  membership: {
             |    resource: "Users",
             |    predicate: 'user => user.isAdmin'
             |  },
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |
             |Role.create({
             |  name: "NormalUsers",
             |  membership: {
             |    resource: "Users",
             |    predicate: 'user => !user.isAdmin'
             |  },
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})""".stripMargin
        )

        val adminUser =
          evalOk(admin, "Users.create({ isAdmin: true })").to[Value.Doc]
        val normalUser =
          evalOk(admin, "Users.create({ isAdmin: false })").to[Value.Doc]

        mkCredentials(admin, adminUser, "Users", "sekret")
        mkCredentials(admin, normalUser, "Users", "sekret")

        val adminToken = login(admin, adminUser, "Users", "sekret")
        val normalToken = login(admin, normalUser, "Users", "sekret")

        evalOk(adminToken, "Users.create({})")
        evalErr(adminToken, "Books.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(normalToken, "Users.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
        evalOk(normalToken, "Books.create({})")
      }
    }

    "access providers" - {
      "works" in {
        val admin = newDB.withPermissions(AdminPermissions)

        evalOk(admin, "Collection.create({ name: 'Books' })")
        evalOk(admin, "Collection.create({ name: 'Users' })")

        evalOk(
          admin,
          """|Role.create({
             |  name: "CreateBooks",
             |  privileges: {
             |    resource: "Books",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |
             |Role.create({
             |  name: "CreateUsers",
             |  privileges: {
             |    resource: "Users",
             |    actions: {
             |      create: true
             |    }
             |  }
             |})
             |""".stripMargin
        )

        val key = aJWK sample
        val issuer = "https://fauna.auth0.com"
        val jwksUri = s"$issuer/.well-known/jwks.json"

        val db = (ctx ! Database.forScope(admin.scopeID)).get
        val audience = JWTToken.canonicalDBUrl(db)

        evalOk(
          admin,
          s"""|AccessProvider.create({
              |  name: "anAccessProvider",
              |  issuer: "$issuer",
              |  jwks_uri: "$jwksUri",
              |  roles: [{
              |    role: "CreateBooks",
              |    predicate: 'payload => "CreateBooks" == payload.sub'
              |  }, {
              |    role: "CreateUsers",
              |    predicate: 'payload => "CreateUsers" == payload.sub'
              |  }]
              |})""".stripMargin
        )

        val provider = jwkProvider(jwksUri, key)

        noException shouldBe thrownBy {
          val jwt = JWT.createToken(
            JSObject(
              JWTFields.Issuer -> issuer,
              JWTFields.Audience -> audience,
              JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
              JWTFields.Subject -> "CreateBooks"
            ),
            key,
            "RS256"
          )

          val jwtAuth = (ctx ! Auth.fromJwt(jwt.getToken, provider)).value
          evalOk(jwtAuth, "Books.create({})")
          evalErr(jwtAuth, "Users.create({})") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "permission_denied",
                  "Insufficient privileges to perform the action.",
                  _,
                  Seq()) =>
          }
        }

        noException shouldBe thrownBy {
          val jwt = JWT.createToken(
            JSObject(
              JWTFields.Issuer -> issuer,
              JWTFields.Audience -> audience,
              JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
              JWTFields.Subject -> "CreateUsers"
            ),
            key,
            "RS256"
          )

          val jwtAuth = (ctx ! Auth.fromJwt(jwt.getToken, provider)).value
          evalErr(jwtAuth, "Books.create({})") should matchPattern {
            case QueryRuntimeFailure.Simple(
                  "permission_denied",
                  "Insufficient privileges to perform the action.",
                  _,
                  Seq()) =>
          }

          evalOk(jwtAuth, "Users.create({})")
        }

        // invalid token doesn't authenticate
        val jwt = JWT.createToken(
          JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.ExpiresAt -> Instant.now().plusSeconds(60).getEpochSecond,
            JWTFields.Subject -> "invalid subject"
          ),
          key,
          "RS256"
        )

        (ctx ! Auth.fromJwt(jwt.getToken, provider)) shouldBe None
      }
    }
  }
}
