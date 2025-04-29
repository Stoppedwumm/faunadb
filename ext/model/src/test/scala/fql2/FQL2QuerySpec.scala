package fauna.model.test

import fauna.atoms.{ DocID, SubID }
import fauna.auth.{ AdminPermissions, Auth, JWTToken }
import fauna.codex.json.JSObject
import fauna.model.runtime.fql2.{ FQLInterpreter, QueryRuntimeFailure }
import fauna.model.Database
import fauna.net.security.{ JWT, JWTFields }
import fauna.repo.values.Value
import fql.ast.{ Span, Src }
import java.time.Instant

class FQL2QuerySpec extends FQL2Spec with AuthGenerators {
  "FQL2QuerySpec" - {
    "isEnvTypechecked" in {
      val db = Auth.adminForScope(newDB.scopeID)

      evalOk(db, "Database.create({ name: 'TypecheckEnabled', typechecked: true })")
      evalOk(
        db,
        "Database.create({ name: 'TypecheckDisabled', typechecked: false })")

      val typecheckEnabledDb =
        Auth.adminForScope(getDB(db.scopeID, "TypecheckEnabled").get.scopeID)
      val typecheckDisabledDb =
        Auth.adminForScope(getDB(db.scopeID, "TypecheckDisabled").get.scopeID)

      evalOk(
        typecheckEnabledDb,
        "Database.create({ name: 'InheritTypecheckEnabled' })")
      evalOk(
        typecheckDisabledDb,
        "Database.create({ name: 'InheritTypecheckDisabled' })")

      val typecheckInheritEnabledDb =
        Auth.adminForScope(
          getDB(typecheckEnabledDb.scopeID, "InheritTypecheckEnabled").get.scopeID)
      val typecheckInheritDisabledDb =
        Auth.adminForScope(
          getDB(typecheckDisabledDb.scopeID, "InheritTypecheckDisabled").get.scopeID)

      evalOk(typecheckEnabledDb, "Query.isEnvTypechecked()") shouldBe Value.True
      evalOk(typecheckDisabledDb, "Query.isEnvTypechecked()") shouldBe Value.False
      evalOk(
        typecheckInheritEnabledDb,
        "Query.isEnvTypechecked()") shouldBe Value.True
      evalOk(
        typecheckInheritDisabledDb,
        "Query.isEnvTypechecked()") shouldBe Value.False
    }

    "isEnvProtected" in {
      val db = Auth.adminForScope(newDB.scopeID)

      evalOk(db, "Database.create({ name: 'ProtectedEnabled', protected: true })")
      evalOk(db, "Database.create({ name: 'ProtectedDisabled', protected: false })")

      val protectedEnabledDb =
        Auth.adminForScope(getDB(db.scopeID, "ProtectedEnabled").get.scopeID)
      val protectedDisabledDb =
        Auth.adminForScope(getDB(db.scopeID, "ProtectedDisabled").get.scopeID)

      evalOk(
        protectedEnabledDb,
        "Database.create({ name: 'InheritProtectedEnabled' })")
      evalOk(
        protectedDisabledDb,
        "Database.create({ name: 'InheritProtectedDisabled' })")

      val protectedInheritEnabledDb =
        Auth.adminForScope(
          getDB(protectedEnabledDb.scopeID, "InheritProtectedEnabled").get.scopeID)
      val protectedInheritDisabledDb =
        Auth.adminForScope(
          getDB(protectedDisabledDb.scopeID, "InheritProtectedDisabled").get.scopeID)

      evalOk(protectedEnabledDb, "Query.isEnvProtected()") shouldBe Value.True
      evalOk(protectedDisabledDb, "Query.isEnvProtected()") shouldBe Value.False
      evalOk(protectedInheritEnabledDb, "Query.isEnvProtected()") shouldBe Value.True
      evalOk(
        protectedInheritDisabledDb,
        "Query.isEnvProtected()") shouldBe Value.False
    }

    "isEnvProtected only works with admin key" in {
      val db = newDB

      evalErr(db, "Query.isEnvProtected()") shouldBe QueryRuntimeFailure
        .PermissionDenied(
          FQLInterpreter.StackTrace(Seq(Span(20, 22, Src.Query("")))))

      val adminKey = Auth.adminForScope(db.scopeID)
      evalOk(adminKey, "Query.isEnvProtected()")
    }

    "Query.identity" - {
      def testIdentity(
        prefix: String,
        setupFn: ((Auth, String, Option[String]) => Unit) => Unit) = {
        s"${prefix}keys - don't have identity" in {
          setupFn { (auth, query, _) =>
            val (_, secret) = createKey(auth)

            evalOk(secret, query) should matchPattern {
              case Value.Null(
                    Value.Null.Cause.NoSuchElement("missing identity", _)) =>
            }
          }
        }

        s"${prefix}keys - can override identity" in {
          setupFn { (auth, query, _) =>
            val (user, secret) = createKey(auth)

            evalOk(s"$secret:@doc/Users/$user", query) should matchPattern {
              case Value.Doc(DocID(SubID(`user`), _), _, _, _, _) =>
            }
          }
        }

        s"${prefix}tokens - works" in {
          setupFn { (auth, query, _) =>
            val (user, secret, _) = createToken(auth)

            evalOk(secret, query) should matchPattern {
              case Value.Doc(DocID(SubID(`user`), _), _, _, _, _) =>
            }
          }
        }

        s"${prefix}tokens - cannot override identity" in {
          setupFn { (auth, query, _) =>
            val (user, secret, _) = createToken(auth)

            val user2 = evalOk(auth, """Users.create({}).id""").as[Long]

            evalOk(s"$secret:@doc/Users/$user2", query) should matchPattern {
              case Value.Doc(DocID(SubID(`user`), _), _, _, _, _) =>
            }
          }
        }
      }

      testIdentity(prefix = "", setupColl("Query.identity()"))
      testIdentity(prefix = "UDF + ", setupUDF("Query.identity()"))

      "works in predicate" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Users"})""")
        evalOk(auth, """Collection.create({name: "Books"})""")

        val user0 = evalOk(auth, """Users.create({}).id""").as[Long]
        val user1 = evalOk(auth, """Users.create({}).id""").as[Long]

        val token0 = evalOk(
          auth,
          s"""|Token.create({
              |  document: Users.byId('$user0')
              |})""".stripMargin
        )

        val token1 = evalOk(
          auth,
          s"""|Token.create({
              |  document: Users.byId('$user1')
              |})""".stripMargin
        )

        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|Role.create({
              |  name: "onlyUser0",
              |  membership: {
              |    resource: "Users",
              |    predicate: "ref => Users.byId('$user0') == Query.identity()"
              |  },
              |  privileges: {
              |    resource: "Books",
              |    actions: {
              |      create: true
              |    }
              |  }
              |})""".stripMargin
        )

        evalOk(
          (getDocFields(auth, token0) / "secret").as[String],
          "Books.create({})") should matchPattern { case Value.Doc(_, _, _, _, _) =>
        }

        evalErr(
          (getDocFields(auth, token1) / "secret").as[String],
          "Books.create({})") should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }
    }

    "Query.token" - {
      def testToken(
        prefix: String,
        setupFn: ((Auth, String, Option[String]) => Unit) => Unit) = {
        s"${prefix}keys - returns null" in {
          setupFn { (auth, query, _) =>
            val (_, secret) = createKey(auth)

            evalOk(secret, query) should matchPattern {
              case Value.Null(Value.Null.Cause.NoSuchElement("no token", _)) =>
            }
          }
        }

        s"${prefix}tokens - works" in {
          setupFn { (auth, query, _) =>
            val (_, secret, token) = createToken(auth)

            evalOk(secret, query) should matchPattern {
              case Value.Doc(DocID(SubID(`token`), _), _, _, _, _) =>
            }
          }
        }

        s"${prefix}JWT - works" in {
          setupFn { (auth, query, role) =>
            val (payload, jwt, provider) = createJWT(auth, role)

            val jwtAuth = (ctx ! Auth.fromJwt(jwt, provider)).value

            evalOk(jwtAuth, query) shouldBe payload
          }
        }
      }

      testToken(prefix = "", setupColl("Query.token()"))
      testToken(prefix = "UDF + ", setupUDF("Query.token()"))
    }

    def createKey(auth: Auth) = {
      val user = evalOk(auth, """Users.create({}).id""").as[Long]

      val key = evalOk(
        auth.withPermissions(AdminPermissions),
        s"""Key.create({role: "admin"})"""
      )

      val secret = (getDocFields(auth, key) / "secret").as[String]

      (user, secret)
    }

    def createToken(auth: Auth) = {
      val user = evalOk(auth, """Users.create({}).id""").as[Long]

      val token = evalOk(
        auth,
        s"""|Token.create({
            |  document: Users.byId('$user')
            |})""".stripMargin
      )
      val fields = getDocFields(auth, token)

      (user, (fields / "secret").as[String], (fields / "id").as[Long])
    }

    def createJWT(auth: Auth, role: Option[String]) = {
      val key = aJWK sample
      val issuer = "https://fauna.auth0.com"
      val jwksUri = s"$issuer/.well-known/jwks.json"

      val db = (ctx ! Database.forScope(auth.scopeID)).value
      val audience = JWTToken.canonicalDBUrl(db)

      evalOk(
        auth.withPermissions(AdminPermissions),
        s"""|Role.create({
            |  name: "aRole",
            |  privileges: {
            |    resource: "Users",
            |    actions: {}
            |  }
            |})
            |
            |AccessProvider.create({
            |  name: "anAccessProvider",
            |  issuer: "$issuer",
            |  jwks_uri: "$jwksUri",
            |  roles: '${role.getOrElse("aRole")}'
            |})""".stripMargin
      )

      val provider = jwkProvider(jwksUri, key)

      val subject = "identity string"
      val expiresAt = Instant.now().plusSeconds(60).getEpochSecond

      val jwt = JWT.createToken(
        JSObject(
          JWTFields.Issuer -> issuer,
          JWTFields.Audience -> audience,
          JWTFields.ExpiresAt -> expiresAt,
          JWTFields.Subject -> subject
        ),
        key,
        "RS256"
      )

      val payload = Value.Struct(
        JWTFields.Issuer -> Value.Str(issuer),
        JWTFields.Audience -> Value.Str(audience),
        JWTFields.ExpiresAt -> Value.Long(expiresAt),
        JWTFields.Subject -> Value.Str(subject)
      )

      (payload, jwt.getToken, provider)
    }

    def setupColl(query: String)(fn: (Auth, String, Option[String]) => Unit) = {
      val auth = newDB

      evalOk(auth, """Collection.create({name: "Users"})""")

      fn(auth, query, None)
    }

    def setupUDF(body: String)(fn: (Auth, String, Option[String]) => Unit) = {
      setupColl(body) { (auth, _, _) =>
        evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|Function.create({
              |  name: "userFunc",
              |  body: "() => $body"
              |})
              |
              |Role.create({
              |  name: "callFunc",
              |  membership: {
              |    resource: "Users"
              |  },
              |  privileges: [{
              |    resource: "userFunc",
              |    actions: {
              |      call: true
              |    }
              |  }]
              |})
              |""".stripMargin
        )

        fn(auth, "userFunc()", Some("callFunc"))
      }
    }
  }
}
