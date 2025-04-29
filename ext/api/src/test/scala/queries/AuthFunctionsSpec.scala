package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.net.security.{ JWT, JWTFields }
import fauna.prop.Prop
import fauna.prop.api.{ AccessProviderRole, Membership, Privilege, RoleAction }
import java.time.Instant
import org.scalatest.BeforeAndAfterAll

class AuthFunctionsSpec extends QueryAPI21Spec {

  "login" - {
    prop("creates tokens") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        pass <- Prop.string
      } {
        val res =
          runQuery(CreateF(cls.refObj,
                           MkObject("data" -> MkObject("name" -> "Hen Wen"),
                                    "credentials" -> MkObject("password" -> pass))),
                   db)

        runQuery(Login(res.refObj, MkObject("password" -> pass)), db)
        qassertErr(Login(res.refObj, Quote(JSObject.empty)),
                   "authentication failed",
                   JSArray(),
                   db)
        qassertErr(Login(res.refObj, MkObject("password" -> "wrong")),
                   "authentication failed",
                   JSArray(),
                   db)
        qassertErr(Login(inst.refObj, MkObject("password" -> "nocreds")),
                   "authentication failed",
                   JSArray(),
                   db)
      }
    }
  }

  "identify" - {
    prop("verifies credentials") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        user <- aDocument(cls)
        nonuser <- aDocument(cls)
        pass <- Prop.string
        _ <- mkCredentials(db, user, pass)
      } {
        runQuery(Identify(user.refObj, pass), db) should equal(JSTrue)
        runQuery(Identify(user.refObj, "wrong"), db) should equal(JSFalse)
        runQuery(Identify(nonuser.refObj, "nocreds"), db) should equal(JSFalse)
      }
    }
  }

  "logout" - {
    prop("destroys tokens") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <- aDocument(cls)
        pass <- Prop.string
        _ <- mkCredentials(db, inst, pass)
      } {
        val tok1 =
          (runQuery(Login(inst.refObj, MkObject("password" -> pass)), db) / "secret")
            .as[String]
        val tok2 =
          (runQuery(Login(inst.refObj, MkObject("password" -> pass)), db) / "secret")
            .as[String]
        val tok3 =
          (runQuery(Login(inst.refObj, MkObject("password" -> pass)), db) / "secret")
            .as[String]

        runQuery(Logout(false), tok1)
        beforeTTLCacheExpiration {
          runRawQuery(Get(Ref("tokens/self")), tok1) should respond(401)
        }
        runQuery(Get(Ref("tokens/self")), tok2)
        runQuery(Get(Ref("tokens/self")), tok3)

        runQuery(Logout(true), tok2)
        beforeTTLCacheExpiration {
          runRawQuery(Get(Ref("tokens/self")), tok2) should respond(401)
        }
        runRawQuery(Get(Ref("tokens/self")), tok3) should respond(401)
      }
    }
  }
}

class AuthFunctionsV4Spec extends QueryAPI4Spec with BeforeAndAfterAll {

  val jwksProvider = new JWKSHTTPServer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    jwksProvider.start()
  }

  override def afterAll(): Unit = {
    jwksProvider.stop()
    super.afterAll()
  }

  "current_token" - {

    prop("tokens") {
      for {
        db <- aDatabase
        user <- aUser(db)
      } {
        val token = user.tokenRes

        val ret = runQuery(Get(CurrentToken()), user.token)
        (ret / "ref") shouldBe (token / "ref")
      }
    }

    prop("keys") {
      for {
        db <- aDatabase
        key <- aKey(db, roleProp = Prop.const("admin"))
      } {
        val ret = runQuery(Get(CurrentToken()), key.secret)
        (ret / "ref") shouldBe key.refObj
      }
    }

    prop("cannot access key from different scope") {
      for {
        db <- aDatabase
        subDB <- aDatabase(apiVers, db)
        key <- aKey(db, Some(subDB), Prop.const("admin"))
      } {
        qassertErr(
          CurrentToken(),
          "invalid token",
          "Token metadata is not accessible.",
          JSArray.empty,
          key.secret
        )
      }
    }

    prop("jwt") {
      for {
        db <- aDatabase
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          JWTFields.Scope)

        val token = runQuery(CurrentToken(), jwt)

        (token / JWTFields.Issuer) shouldBe JSString(issuer)
        (token / JWTFields.Audience) shouldBe JSString(audience)
        (token / JWTFields.IssuedAt) shouldBe JSLong(issuedAt.getEpochSecond)
        (token / JWTFields.ExpiresAt) shouldBe JSLong(expireAt.getEpochSecond)
        (token / JWTFields.Scope) shouldBe JSString(JWTFields.Scope)
      }
    }

    prop("works on roles privileges predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        user <- aUser(db)
        _ <- aRole(
          db,
          Membership(user.collection),
          Privilege(
            coll.refObj,
            create = RoleAction(Lambda("data" -> Equals(CurrentToken(), user.tokenRes.refObj)))))
      } {
        runRawQuery(CreateF(coll.refObj), user) should respond(Created)
      }
    }

    prop("works on roles membership predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        user <- aUser(db)
        _ <- aRole(
          db,
          Membership(user.collection, Lambda("ref" -> Equals(CurrentToken(), user.tokenRes.refObj))),
          Privilege(coll.refObj, create = RoleAction.Granted))
      } {
        runRawQuery(CreateF(coll.refObj), user) should respond(Created)
      }
    }

    prop("works on access provider predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(coll.refObj, create = RoleAction.Granted))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role, Lambda("token" -> Equals(CurrentToken(), Var("token"))))))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          JWTFields.Scope)

        runRawQuery(CreateF(coll.refObj), jwt) should respond(Created)
      }
    }

    prop("works with UDF + tokens") {
      for {
        db <- aDatabase
        user <- aUser(db)
        fn <- aFunc(db, body = Lambda("_" -> CurrentToken()), role = "server")
        _ <- aRole(
          db,
          Membership(user.collection),
          Privilege(fn.refObj, call = RoleAction.Granted)
        )
      } {
        val token = user.tokenRes

        qequals(
          Call(fn.refObj),
          token / "ref",
          user
        )
      }
    }

    prop("works with UDF + keys") {
      for {
        db <- aDatabase
        key <- aKey(db, roleProp = Prop.const("admin"))
        fn <- aFunc(db, body = Lambda("_" -> CurrentToken()), role = "server")
      } {
        qequals(
          Call(fn.refObj),
          key.refObj,
          key.secret
        )
      }
    }

    prop("works with UDF + jwt") {
      for {
        db <- aDatabase
        fn <- aFunc(db, body = Lambda("_" -> CurrentToken()), role = "server")
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(fn.refObj, call = RoleAction.Granted)
        )
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role))
        )
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          JWTFields.Scope)

        val token = runQuery(Call(fn.refObj), jwt)

        (token / JWTFields.Issuer) shouldBe JSString(issuer)
        (token / JWTFields.Audience) shouldBe JSString(audience)
        (token / JWTFields.IssuedAt) shouldBe JSLong(issuedAt.getEpochSecond)
        (token / JWTFields.ExpiresAt) shouldBe JSLong(expireAt.getEpochSecond)
        (token / JWTFields.Scope) shouldBe JSString(JWTFields.Scope)
      }
    }
  }

  "has_current_token" - {

    prop("tokens") {
      for {
        db <- aDatabase
        user <- aUser(db)
      } {
        qassert(HasCurrentToken(), user.token)
      }
    }

    prop("keys") {
      for {
        db <- aDatabase
        subDB <- aDatabase(apiVers, db)
        key0 <- aKey(db, roleProp = Prop.const("admin"))
        key1 <- aKey(db, Some(subDB), Prop.const("admin"))
      } {
        qassert(HasCurrentToken(), key0.secret)

        //key lives in a different scope it grant access to
        qassert(Not(HasCurrentToken()), key1.secret)
      }
    }

    prop("jwt") {
      for {
        db <- aDatabase
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
          Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role)))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          JWTFields.Scope)

        qassert(HasCurrentToken(), jwt)
      }
    }

    prop("works on roles privileges predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        user <- aUser(db)
        _ <- aRole(
          db,
          Membership(user.collection),
          Privilege(
            coll.refObj,
            create = RoleAction(Lambda("data" -> HasCurrentToken()))))
      } {
        runRawQuery(CreateF(coll.refObj), user) should respond(Created)
      }
    }

    prop("works on roles membership predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        user <- aUser(db)
        _ <- aRole(
          db,
          Membership(user.collection, Lambda("ref" -> HasCurrentToken())),
          Privilege(coll.refObj, create = RoleAction.Granted))
      } {
        runRawQuery(CreateF(coll.refObj), user) should respond(Created)
      }
    }

    prop("works on access provider predicates") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        key <- aJWK
        userName <- aName
        issuer = s"https://auth.fauna.com/$userName"
        jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
        role <- aRole(
          db,
          Privilege(coll.refObj, create = RoleAction.Granted))
        _ <- anAccessProvider(
          db,
          Prop.const(issuer),
          Prop.const(jwksUrl),
          Seq(AccessProviderRole(role, Lambda("token" -> HasCurrentToken()))))
      } {
        jwksProvider.add(jwksUrl, key)

        val audience = s"https://db.fauna.com/db/${db.globalID}"

        val issuedAt = Instant.now()
        val expireAt = issuedAt.plusSeconds(60)

        val jwt = JWT.createToken(
          key,
          "RS256",
          issuer,
          audience,
          issuedAt.getEpochSecond,
          expireAt.getEpochSecond,
          JWTFields.Scope)

        runRawQuery(CreateF(coll.refObj), jwt) should respond(Created)
      }
    }
  }

  Seq("current_identity" -> CurrentIdentity(), "identity" -> Identity()) foreach { case (name, identityFunction) =>
    name - {
      prop("tokens") {
        for {
          db <- aDatabase
          user <- aUser(db)
        } {
          qequals(
            identityFunction,
            user.refObj,
            user
          )
        }
      }

      prop("keys") {
        for {
          db <- aDatabase
          key <- aKey(db, roleProp = Prop.const("admin"))
          coll <- aCollection(db)
          doc <- aDocument(coll)
        } {
          qassertErr(
            identityFunction,
            "missing identity",
            "Authentication does not contain an identity",
            JSArray.empty,
            key.secret
          )

          //override key's scope with an identity
          qequals(
            identityFunction,
            doc.refObj,
            s"${key.secret}:@doc/${coll.name}/${doc.id}"
          )
        }
      }

      prop("jwt") {
        for {
          db <- aDatabase
          key <- aJWK
          userName <- aName
          issuer = s"https://auth.fauna.com/$userName"
          jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
          role <- aRole(
            db,
            Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
            Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
          _ <- anAccessProvider(
            db,
            Prop.const(issuer),
            Prop.const(jwksUrl),
            Seq(AccessProviderRole(role)))
        } {
          jwksProvider.add(jwksUrl, key)

          val audience = s"https://db.fauna.com/db/${db.globalID}"

          val issuedAt = Instant.now()
          val expireAt = issuedAt.plusSeconds(60)

          val payload0 = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond,
            JWTFields.Subject -> "fauna|123456789"
          )

          val jwt0 = JWT.createToken(payload0, key, "RS256")

          qequals(
            identityFunction,
            "fauna|123456789",
            jwt0
          )

          // payload without sub field
          val payload1 = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond
          )

          val jwt1 = JWT.createToken(payload1, key, "RS256")

          qassertErr(
            identityFunction,
            "missing identity",
            "Authentication does not contain an identity",
            JSArray.empty,
            jwt1
          )
        }
      }

      prop("works on roles privileges predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          user <- aUser(db)
          _ <- aRole(
            db,
            Membership(user.collection),
            Privilege(
              coll.refObj,
              create = RoleAction(Lambda("data" -> Equals(identityFunction, user.refObj)))))
        } {
          runRawQuery(CreateF(coll.refObj), user) should respond(Created)
        }
      }

      prop("works on roles membership predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          user <- aUser(db)
          _ <- aRole(
            db,
            Membership(user.collection, Lambda("ref" -> Equals(identityFunction, user.refObj))),
            Privilege(coll.refObj, create = RoleAction.Granted))
        } {
          runRawQuery(CreateF(coll.refObj), user) should respond(Created)
        }
      }

      prop("works on access provider predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          key <- aJWK
          userName <- aName
          issuer = s"https://auth.fauna.com/$userName"
          jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
          role <- aRole(
            db,
            Privilege(coll.refObj, create = RoleAction.Granted))
          _ <- anAccessProvider(
            db,
            Prop.const(issuer),
            Prop.const(jwksUrl),
            Seq(AccessProviderRole(role, Lambda("token" -> Equals(identityFunction, "fauna|123456789")))))
        } {
          jwksProvider.add(jwksUrl, key)

          val audience = s"https://db.fauna.com/db/${db.globalID}"

          val issuedAt = Instant.now()
          val expireAt = issuedAt.plusSeconds(60)

          val payload = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond,
            JWTFields.Subject -> "fauna|123456789"
          )

          val jwt = JWT.createToken(payload, key, "RS256")

          runRawQuery(CreateF(coll.refObj), jwt) should respond(Created)
        }
      }

      prop("works with UDF + tokens") {
        for {
          db <- aDatabase
          user <- aUser(db)
          fn <- aFunc(db, body = Lambda("_" -> identityFunction), role = "server")
          _ <- aRole(
            db,
            Membership(user.collection),
            Privilege(fn.refObj, call = RoleAction.Granted)
          )
        } {
          qequals(
            Call(fn.refObj),
            user.refObj,
            user
          )
        }
      }

      prop("works with UDF + keys") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          doc <- aDocument(coll)
          key <- aKey(db, roleProp = Prop.const("admin"))
          fn <- aFunc(db, body = Lambda("_" -> identityFunction), role = "server")
          _ <- aRole(
            db,
            Membership(coll),
            Privilege(fn.refObj, call = RoleAction.Granted)
          )
        } {
          qequals(
            Call(fn.refObj),
            doc.refObj,
            s"${key.secret}:@doc/${coll.name}/${doc.id}"
          )
        }
      }

      prop("works with UDF + jwt") {
        for {
          db <- aDatabase
          fn <- aFunc(db, body = Lambda("_" -> identityFunction), role = "server")
          key <- aJWK
          userName <- aName
          issuer = s"https://auth.fauna.com/$userName"
          jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
          role <- aRole(
            db,
            Privilege(fn.refObj, call = RoleAction.Granted)
          )
          _ <- anAccessProvider(
            db,
            Prop.const(issuer),
            Prop.const(jwksUrl),
            Seq(AccessProviderRole(role))
          )
        } {
          jwksProvider.add(jwksUrl, key)

          val audience = s"https://db.fauna.com/db/${db.globalID}"

          val issuedAt = Instant.now()
          val expireAt = issuedAt.plusSeconds(60)

          val payload = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond,
            JWTFields.Subject -> "fauna|123456789"
          )

          val jwt = JWT.createToken(payload, key, "RS256")

          runQuery(Call(fn.refObj), jwt) shouldBe JSString("fauna|123456789")
        }
      }
    }
  }

  Seq("has_current_identity" -> HasCurrentIdentity(), "has_identity" -> HasIdentity()) foreach { case (name, hasIdentityFunction) =>
    name - {
      prop("tokens") {
        for {
          db <- aDatabase
          user <- aUser(db)
        } {
          qassert(hasIdentityFunction, user)
        }
      }

      prop("keys") {
        for {
          db <- aDatabase
          key <- aKey(db, roleProp = Prop.const("admin"))
          coll <- aCollection(db)
          doc <- aDocument(coll)
        } {
          qassert(Not(hasIdentityFunction), key.secret)

          qassert(hasIdentityFunction, s"${key.secret}:@doc/${coll.name}/${doc.id}")
        }
      }

      prop("jwt") {
        for {
          db <- aDatabase
          key <- aJWK
          userName <- aName
          issuer = s"https://auth.fauna.com/$userName"
          jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
          role <- aRole(
            db,
            Privilege(ClassesNativeClassRef, create = RoleAction.Granted),
            Privilege(DatabaseNativeClassRef, create = RoleAction.Denied))
          _ <- anAccessProvider(
            db,
            Prop.const(issuer),
            Prop.const(jwksUrl),
            Seq(AccessProviderRole(role)))
        } {
          jwksProvider.add(jwksUrl, key)

          val audience = s"https://db.fauna.com/db/${db.globalID}"

          val issuedAt = Instant.now()
          val expireAt = issuedAt.plusSeconds(60)

          val payload0 = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond,
            JWTFields.Subject -> "fauna|123456789"
          )

          val jwt0 = JWT.createToken(payload0, key, "RS256")

          qassert(hasIdentityFunction, jwt0)

          // payload without sub field
          val payload1 = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond
          )

          val jwt1 = JWT.createToken(payload1, key, "RS256")

          qassert(Not(hasIdentityFunction), jwt1)
        }
      }

      prop("works on roles privileges predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          user <- aUser(db)
          _ <- aRole(
            db,
            Membership(user.collection),
            Privilege(
              coll.refObj,
              create = RoleAction(Lambda("data" -> hasIdentityFunction))))
        } {
          runRawQuery(CreateF(coll.refObj), user) should respond(Created)
        }
      }

      prop("works on roles membership predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          user <- aUser(db)
          _ <- aRole(
            db,
            Membership(user.collection, Lambda("ref" -> hasIdentityFunction)),
            Privilege(coll.refObj, create = RoleAction.Granted))
        } {
          runRawQuery(CreateF(coll.refObj), user) should respond(Created)
        }
      }

      prop("works on access provider predicates") {
        for {
          db <- aDatabase
          coll <- aCollection(db)
          key <- aJWK
          userName <- aName
          issuer = s"https://auth.fauna.com/$userName"
          jwksUrl = s"${jwksProvider.address}/$userName/.well-known/jwks.json"
          role <- aRole(
            db,
            Privilege(coll.refObj, create = RoleAction.Granted))
          _ <- anAccessProvider(
            db,
            Prop.const(issuer),
            Prop.const(jwksUrl),
            Seq(AccessProviderRole(role, Lambda("token" -> hasIdentityFunction))))
        } {
          jwksProvider.add(jwksUrl, key)

          val audience = s"https://db.fauna.com/db/${db.globalID}"

          val issuedAt = Instant.now()
          val expireAt = issuedAt.plusSeconds(60)

          val payload = JSObject(
            JWTFields.Issuer -> issuer,
            JWTFields.Audience -> audience,
            JWTFields.IssuedAt -> issuedAt.getEpochSecond,
            JWTFields.ExpiresAt -> expireAt.getEpochSecond,
            JWTFields.Subject -> "fauna|123456789"
          )

          val jwt = JWT.createToken(payload, key, "RS256")

          runRawQuery(CreateF(coll.refObj), jwt) should respond(Created)
        }
      }
    }
  }
}
