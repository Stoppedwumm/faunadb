package fauna.model.auth.test

import fauna.ast._
import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model._
import fauna.model.test._
import fauna.net.http._
import fauna.net.security.JWT
import fauna.prop.Generators
import fauna.repo.test.CassandraHelper
import fauna.util.BCrypt
import scala.concurrent.duration._

class AuthSpec extends Spec with Generators with AuthGenerators {
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")

  val scope = ctx ! newScope
  val auth = Auth.forScope(scope)
  val db = (ctx ! Database.forScope(scope)).get

  val root = "rootkey"
  val rootHash = BCrypt.hash(root)

  def authLookup(auth: AuthType): Option[Auth] =
    ctx ! Auth.fromInfo(auth, List(rootHash), jwkProvider = null)

  def makeKey(db: String, role: String) = {
    ctx ! mkKey(db, role)
  }

  "Auth.lookup" - {
    "can lookup by secret" in {
      val (keySecret, authKey) = makeKey(db.name, "server")
      val auth = authLookup(keySecret)
      auth should equal(
        Some(ctx ! LoginAuth.get(scope, ServerPermissions, KeyLogin(authKey))))
    }

    "lookup scoped secrets with special chars" in {
      val dbName = "db.with.dots.in.the.name"

      val child = ctx ! newScope(Auth.adminForScope(scope), dbName)

      val (keySecret, authKey) = makeKey(db.name, "admin")

      val secret = s"$keySecret:$dbName:server"

      authLookup(BearerAuth(secret)) shouldBe Some(
        ctx ! LoginAuth
          .get(child, ServerPermissions, KeyLogin(authKey), authDatabase = Some(db)))
    }

    "can set an instance scope" in {
      val (serverSecret, serverAuth) = makeKey(db.name, "server")
      val (adminSecret, adminAuth) = makeKey(db.name, "admin")
      val (clientSecret, _) = makeKey(db.name, "client")
      val (serverReadOnlySecret, _) = makeKey(db.name, "server-readonly")

      val collName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      val inst = ctx ! mkDoc(auth, collName)

      // only server and admin keys
      val perms = InstancePermissions(inst.id, RoleEvalContext.NullContext)
      authLookup(s"$serverSecret:${inst.ref}") shouldBe Some(
        ctx ! LoginAuth
          .get(scope, perms, KeyLogin(serverAuth), identity = Some(inst.id)))
      authLookup(s"$adminSecret:${inst.ref}") shouldBe Some(
        ctx ! LoginAuth
          .get(scope, perms, KeyLogin(adminAuth), identity = Some(inst.id)))

      // client and server-readonly are forbidden
      authLookup(s"$clientSecret:${inst.ref}") shouldBe None
      authLookup(s"$serverReadOnlySecret:${inst.ref}") shouldBe None
    }

    "can reach into a sub-database" in {
      val rootDB = (ctx ! Database.forScope(ScopeID.RootID)).get

      val (secret, authKey) = makeKey(db.name, "admin")

      val (nonPermissiveSecret, _) = (ctx ! mkKey(db.name, "server"))

      val sub1 = ctx ! newScope(Auth.adminForScope(scope))
      val sub1DB = (ctx ! Database.forScope(sub1)).get

      val sub2 = ctx ! newScope(Auth.adminForScope(sub1))
      val sub2Auth = Auth.forScope(sub2)
      val sub2DB = (ctx ! Database.forScope(sub2)).get

      ctx ! mkCollection(sub2Auth, MkObject("name" -> "myclass"))
      val inst = ctx ! mkDoc(sub2Auth, "myclass")

      Seq(
        (
          s"${sub1DB.name}:server",
          ctx ! KeyAuth.get(
            sub1,
            authKey,
            ServerPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub1, ServerPermissions)),
        (
          s"${sub1DB.name}:server-readonly",
          ctx ! KeyAuth.get(
            sub1,
            authKey,
            ServerReadOnlyPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub1, ServerReadOnlyPermissions)),
        (
          s"${sub1DB.name}:admin",
          ctx ! KeyAuth.get(
            sub1,
            authKey,
            AdminPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub1, AdminPermissions)),
        (
          s"${sub1DB.name}:client",
          ctx ! KeyAuth.get(sub1, authKey, NullPermissions, authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub1, NullPermissions)),
        (
          s"${sub1DB.name}/${sub2DB.name}:server",
          ctx ! KeyAuth.get(
            sub2,
            authKey,
            ServerPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub2, ServerPermissions)),
        (
          s"${sub1DB.name}/${sub2DB.name}:server-readonly",
          ctx ! KeyAuth.get(
            sub2,
            authKey,
            ServerReadOnlyPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub2, ServerReadOnlyPermissions)),
        (
          s"${sub1DB.name}/${sub2DB.name}:admin",
          ctx ! KeyAuth.get(
            sub2,
            authKey,
            AdminPermissions,
            authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub2, AdminPermissions)),
        (
          s"${sub1DB.name}/${sub2DB.name}:client",
          ctx ! KeyAuth.get(sub2, authKey, NullPermissions, authDatabase = Some(db)),
          ctx ! SystemAuth.get(sub2, NullPermissions)),
        (
          s"${sub1DB.name}/${sub2DB.name}:${inst.ref}",
          ctx ! LoginAuth.get(
            sub2,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            KeyLogin(authKey),
            identity = Some(inst.id),
            authDatabase = Some(db)),
          ctx ! LoginAuth.get(
            sub2,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            RootLogin,
            identity = Some(inst.id),
            authDatabase = Some(rootDB)))
      ) foreach { case (scope, auth, rootAuth) =>
        authLookup(s"$secret:$scope") should equal(Some(auth))
        authLookup(s"$root:${db.name}/$scope") should equal(Some(rootAuth))
        authLookup(s"$nonPermissiveSecret:$scope") should equal(None)
      }
    }

    "key auth: allow custom role" in {
      val collName = aName.sample
      val roleName = aName.sample

      val sub1 = ctx ! newScope(Auth.adminForScope(scope), APIVersion.V21)
      val sub1Auth = Auth.adminForScope(sub1)
      val sub1DB = (ctx ! Database.forScope(sub1)).get

      val sub2 = ctx ! newScope(sub1Auth, APIVersion.V21)
      val sub2Auth = Auth.adminForScope(sub2)
      val sub2DB = (ctx ! Database.forScope(sub2)).get

      ctx ! mkCollection(sub2Auth, MkObject("name" -> collName))

      val roleID = ctx ! mkRole(
        sub2Auth,
        roleName,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      val roleEvalContext = ctx ! RoleEvalContext.lookup(sub2, Set(roleID))

      val (secret, authKey) =
        ctx ! mkKey(sub1DB.name, "admin", Auth.adminForScope(scope))

      authLookup(s"$secret:${sub2DB.name}:@role/$roleName") shouldBe
        Some(
          ctx ! KeyAuth
            .get(
              sub2,
              authKey,
              RolePermissions(roleEvalContext),
              authDatabase = Some(sub1DB)))
    }

    "key auth: omit database" in {
      val collName = aName.sample
      val roleName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      val inst = ctx ! mkDoc(auth, collName)

      val roleID = ctx ! mkRole(
        Auth.adminForScope(scope),
        roleName,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

      val (secret, authKey) = ctx ! mkKey(db.name, "admin")

      authLookup(s"$secret:admin") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, AdminPermissions))

      authLookup(s"$secret:server") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, ServerPermissions))

      authLookup(s"$secret:server-readonly") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, ServerReadOnlyPermissions))

      authLookup(s"$secret:client") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, NullPermissions))

      authLookup(s"$secret:${inst.ref}") shouldBe
        Some(
          ctx ! LoginAuth.get(
            scope,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            KeyLogin(authKey),
            identity = Some(inst.id)))

      authLookup(s"$secret:@role/$roleName") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, RolePermissions(roleEvalContext)))
    }

    "key auth: only admin keys can override scope" in {
      val collName = aName.sample
      val roleName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))

      val roleID = ctx ! mkRole(
        Auth.adminForScope(scope),
        roleName,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

      val (serverKey, _) = (ctx ! mkKey(db.name, "server"))
      authLookup(s"$serverKey:admin") shouldBe None
      authLookup(s"$serverKey:@role/$roleName") shouldBe None

      val (serverReadOnlyKey, _) = ctx ! mkKey(db.name, "server-readonly")
      authLookup(s"$serverReadOnlyKey:admin") shouldBe None
      authLookup(s"$serverReadOnlyKey:@role/$roleName") shouldBe None

      val (clientKey, _) = ctx ! mkKey(db.name, "client")
      authLookup(s"$clientKey:admin") shouldBe None
      authLookup(s"$clientKey:@role/$roleName") shouldBe None

      val (adminKey, authKey) = ctx ! mkKey(db.name, "admin")

      authLookup(s"$adminKey:client") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, NullPermissions))
      authLookup(s"$adminKey:@role/$roleName") shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, RolePermissions(roleEvalContext)))
    }

    "key auth: custom role keys cannot override scope" in {
      val collName = aName.sample
      val role1Name = aName.sample
      val role2Name = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))

      val role1ID = ctx ! mkRole(
        Auth.adminForScope(scope),
        role1Name,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      ctx ! mkRole(
        Auth.adminForScope(scope),
        role2Name,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> false)
          )))

      val (secret, authKey) =
        ctx ! mkRoleKey(RoleRef(role1Name), Auth.adminForScope(scope))

      val role1EvalContext = ctx ! RoleEvalContext.lookup(scope, Set(role1ID))

      authLookup(secret) shouldBe
        Some(ctx ! KeyAuth.get(scope, authKey, RolePermissions(role1EvalContext)))

      // cannot override role
      authLookup(s"$secret:@role/$role2Name") shouldBe None
    }

    "root auth: allow custom role" in {
      val collName = aName.sample
      val roleName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))

      val roleID = ctx ! mkRole(
        Auth.adminForScope(scope),
        roleName,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      val roleEvalContext = ctx ! RoleEvalContext.lookup(scope, Set(roleID))

      authLookup(s"$root:${db.name}:@role/$roleName") shouldBe
        Some(ctx ! SystemAuth.get(scope, RolePermissions(roleEvalContext)))
    }

    "root auth: omit database" in {
      val collName = aName.sample
      val roleName = aName.sample

      ctx ! mkCollection(RootAuth, MkObject("name" -> collName))
      val inst = ctx ! mkDoc(RootAuth, collName)

      val roleID = ctx ! mkRole(
        RootAuth,
        roleName,
        Seq(
          MkObject(
            "resource" -> ClassRef(collName),
            "actions" -> MkObject("read" -> true)
          )))

      val roleEvalContext = ctx ! RoleEvalContext.lookup(ScopeID.RootID, Set(roleID))

      authLookup(s"$root:admin") shouldBe
        Some(ctx ! SystemAuth.get(ScopeID.RootID, AdminPermissions))

      authLookup(s"$root:server") shouldBe
        Some(ctx ! SystemAuth.get(ScopeID.RootID, ServerPermissions))

      authLookup(s"$root:server-readonly") shouldBe
        Some(ctx ! SystemAuth.get(ScopeID.RootID, ServerReadOnlyPermissions))

      authLookup(s"$root:client") shouldBe
        Some(ctx ! SystemAuth.get(ScopeID.RootID, NullPermissions))

      authLookup(s"$root:${inst.ref}") shouldBe
        Some(
          ctx ! LoginAuth.get(
            ScopeID.RootID,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            RootLogin,
            identity = Some(inst.id)))

      authLookup(s"$root:@role/$roleName") shouldBe
        Some(ctx ! SystemAuth.get(ScopeID.RootID, RolePermissions(roleEvalContext)))
    }

    "token auth: ignore scope" in {
      val collName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      val inst = ctx ! mkDoc(
        auth,
        collName,
        MkObject("credentials" -> MkObject("password" -> "sekrit")))

      val login = ctx ! loginAs(auth, inst, "sekrit")

      val token = (ctx ! Token.get(login.scope, login.id)).get

      authLookup(s"${login.secret}:non-existent-role") shouldBe
        Some(
          ctx ! LoginAuth.get(
            scope,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            TokenLogin(token),
            identity = Some(inst.id)))

      authLookup(
        s"${login.secret}:some/non-sense/database:non-existent-role") shouldBe
        Some(
          ctx ! LoginAuth.get(
            scope,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            TokenLogin(token),
            identity = Some(inst.id)))
    }

    "support @doc ref" in {
      val rootDB = (ctx ! Database.forScope(ScopeID.RootID)).get

      val (secret, authKey) = makeKey(db.name, "server")

      val collName = aName.sample

      ctx ! mkCollection(auth, MkObject("name" -> collName))
      val inst = ctx ! mkDoc(auth, collName)

      val auth2 = authLookup(s"$secret:${inst.docRef}")
      auth2 should equal(
        Some(
          ctx ! LoginAuth.get(
            scope,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            KeyLogin(authKey),
            identity = Some(inst.id))))

      val auth3 = authLookup(s"$root:${db.name}:${inst.docRef}")
      auth3 should equal(
        Some(
          ctx ! LoginAuth.get(
            scope,
            InstancePermissions(inst.id, RoleEvalContext.NullContext),
            RootLogin,
            identity = Some(inst.id),
            authDatabase = Some(rootDB))))
    }

    "revalidates" - {

      "returns true for root auth" in {
        (ctx ! Auth.revalidate(RootAuth)) shouldBe true
      }

      "returns true for root login auth" in {
        val rootLoginAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = RootLogin)
        (ctx ! Auth.revalidate(rootLoginAuth)) shouldBe true
      }

      "returns true for key login if unchanged" in {
        val (_, key) = makeKey(db.name, "admin")
        val keyAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = KeyLogin(key))

        (ctx ! Auth.revalidate(keyAuth)) shouldBe true
      }

      "returns false for key login if changed" in {
        val (secret, key) = makeKey(db.name, "admin")
        val keyAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = KeyLogin(key))

        ctx ! {
          runQuery(
            RootAuth,
            Update(
              Select("ref", KeyFromSecret(secret)),
              MkObject("role" -> "server")
            ))
        }

        (ctx ! Auth.revalidate(keyAuth)) shouldBe false
      }

      "returns false for expired key" in {
        val (secret, key) = makeKey(db.name, "admin")

        ctx ! {
          runQuery(
            RootAuth,
            Update(
              Select("ref", KeyFromSecret(secret)),
              MkObject("ttl" -> TimeSubtract(Now(), 1, "day"))
            ))
        }

        val keyAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = KeyLogin(key))

        (ctx ! Auth.revalidate(keyAuth)) shouldBe false
      }

      "returns false for deleted key" in {
        val (secret, key) = makeKey(db.name, "admin")
        val keyAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = KeyLogin(key))

        ctx ! runQuery(RootAuth, DeleteF(Select("ref", KeyFromSecret(secret))))
        (ctx ! Auth.revalidate(keyAuth)) shouldBe false
      }

      "returns true for token if unchanged" in {
        val collName = aUniqueName.sample
        ctx ! mkCollection(auth, MkObject("name" -> collName))

        val doc = ctx ! {
          mkDoc(
            auth,
            collName,
            MkObject(
              "credentials" -> MkObject(
                "password" -> "sekrit"
              )))
        }

        val token = ctx ! {
          loginAs(auth, doc, "sekrit") flatMap { tok =>
            Token.get(tok.scope, tok.id) getOrElseT fail("no token found")
          }
        }

        val tokenAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = TokenLogin(token))

        (ctx ! Auth.revalidate(tokenAuth)) shouldBe true
      }

      "returns false for deleted token" in {
        val collName = aUniqueName.sample
        ctx ! mkCollection(auth, MkObject("name" -> collName))

        val doc = ctx ! {
          mkDoc(
            auth,
            collName,
            MkObject(
              "credentials" -> MkObject(
                "password" -> "sekrit"
              )))
        }

        val token = ctx ! {
          loginAs(auth, doc, "sekrit") flatMap { tok =>
            Token.get(tok.scope, tok.id) getOrElseT fail("no token found")
          }
        }

        val tokenAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = InstancePermissions(doc.id, RoleEvalContext.NullContext),
            source = TokenLogin(token),
            identity = Some(doc.id)
          )

        ctx ! logoutAs(tokenAuth, removeAll = true)
        (ctx ! Auth.revalidate(tokenAuth)) shouldBe false
      }

      "returns false for expired token" in {
        val collName = aUniqueName.sample
        ctx ! mkCollection(auth, MkObject("name" -> collName))

        val doc = ctx ! {
          mkDoc(
            auth,
            collName,
            MkObject(
              "credentials" -> MkObject(
                "password" -> "sekrit"
              )))
        }

        val token = ctx ! {
          val ttl = Clock.time - 1.day
          loginAs(auth, doc, "sekrit", Some(ttl)) flatMap { tok =>
            Token.get(tok.scope, tok.id) getOrElseT fail("no token found")
          }
        }

        val tokenAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = TokenLogin(token),
            identity = None)

        (ctx ! Auth.revalidate(tokenAuth)) shouldBe false
      }

      "returns true for valid JWT" in {
        val payload = JSObject("exp" -> Timestamp.Max.seconds)
        val jwt = JWT.createToken(payload, aJWK.sample, "RS256")
        val jwtAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = JWTLogin(jwt, NullL),
            identity = None)

        (ctx ! Auth.revalidate(jwtAuth)) shouldBe true
      }

      "returns false for expired JWT" in {
        val payload = JSObject("exp" -> Timestamp.Min.seconds)
        val jwt = JWT.createToken(payload, aJWK.sample, "RS256")
        val jwtAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = AdminPermissions,
            source = JWTLogin(jwt, NullL),
            identity = None)

        (ctx ! Auth.revalidate(jwtAuth)) shouldBe false
      }

      "returns false on ABAC changes" in {
        val collName = aUniqueName.sample
        val roleName = aUniqueName.sample
        ctx ! mkCollection(auth, MkObject("name" -> collName))

        val roleID = ctx ! {
          mkRole(
            Auth.adminForScope(scope),
            roleName,
            Seq(
              MkObject(
                "resource" -> ClassRef(collName),
                "actions" -> MkObject("read" -> true)
              )))
        }

        val roleEvalCtx = ctx ! RolePermissions.lookup(scope, Set(roleID))

        val abacAuth =
          ctx ! LoginAuth.get(
            scopeID = scope,
            permissions = roleEvalCtx,
            source = RootLogin,
            identity = None)

        (ctx ! Auth.revalidate(abacAuth)) shouldBe true

        ctx ! {
          runQuery(
            Auth.adminForScope(scope),
            Update(
              RoleRef(roleName),
              MkObject(
                "privileges" -> Seq(
                  MkObject(
                    "resource" -> ClassRef(collName),
                    "actions" -> MkObject("write" -> true)
                  ))))
          )
        }

        (ctx ! Auth.revalidate(abacAuth)) shouldBe false
      }
    }
  }
}
