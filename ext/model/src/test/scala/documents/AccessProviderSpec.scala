package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.{ Auth, JWTToken }
import fauna.model._
import fauna.prop.Generators
import fauna.repo.test.CassandraHelper
import fauna.storage.doc._
import fauna.storage.ir._

class AccessProviderSpec extends Spec with Generators {

  private val ctx = CassandraHelper.context("model")

  "AccessProviderVersionValidatorSpec" - {
    "require name, issuer and jwks_uri" in {
      patchFailures(MapV.empty) shouldBe List(
        ValueRequired(List("name")),
        ValueRequired(List("issuer")),
        ValueRequired(List("jwks_uri"))
      )
    }

    "jwks_uri must be a valid url" in {
      val data = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "aUrl"
      )

      patchFailures(data) shouldBe List(
        InvalidURI(List("jwks_uri"), "aUrl")
      )
    }

    "roles is optional" in {
      val data = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> NullV
      )

      patched(data) shouldBe MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json"
      )
    }

    "roles can be empty" in {
      val data = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> ArrayV.empty
      )

      patched(data) shouldBe data
    }

    "roles is an object or array of objects" in {
      val lambda = QueryV(APIVersion.V3, MapV("lambda" -> "token", "expr" -> true))

      val data0 = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> MapV(
          "role" -> RoleID(1).toDocID,
          "predicate" -> lambda
        )
      )

      patched(data0) shouldBe data0

      val data1 = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> ArrayV(
          MapV(
            "role" -> RoleID(1).toDocID,
            "predicate" -> lambda
          ))
      )

      patched(data1) shouldBe data1
    }

    "roles is a role reference or array of role references" in {
      val data0 = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> RoleID(1).toDocID
      )

      patched(data0) shouldBe data0

      val data1 = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> ArrayV(RoleID(1).toDocID)
      )

      patched(data1) shouldBe data1
    }

    "roles can mix role reference and object" in {
      val lambda = QueryV(APIVersion.V3, MapV("lambda" -> "token", "expr" -> true))

      val data = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> ArrayV(
          RoleID(1).toDocID,
          MapV(
            "role" -> RoleID(2).toDocID,
            "predicate" -> lambda
          )
        )
      )

      patched(data) shouldBe data
    }

    "predicate cannot be null" in {
      val data = MapV(
        "name" -> "aProvider",
        "issuer" -> "aIssuer",
        "jwks_uri" -> "https://fauna.auth0.com/.well-known/jwks.json",
        "roles" -> ArrayV(
          RoleID(1).toDocID,
          MapV(
            "role" -> RoleID(2).toDocID,
            "predicate" -> NullV
          )
        )
      )

      patchFailures(data) shouldBe List(
        ValueRequired(List("predicate"))
      )
    }
  }

  "AccessProviderSpec" - {
    import SocialHelpersV4._

    val scopeID = ctx ! newScope
    val auth = Auth.adminForScope(scopeID)
    val globalID = {
      val db = ctx ! Database.forScope(scopeID)
      db.get.globalID
    }

    def create(
      provider: String,
      issuer: String,
      url: String = "https://fauna.auth0.com/.well-known/jwks.json",
      roles: Seq[String] = Seq.empty) =
      CreateAccessProvider(
        MkObject(
          "name" -> provider,
          "issuer" -> issuer,
          "jwks_uri" -> url,
          "roles" -> roles
        ))

    "rejects builtin roles" in {
      val provider = aName.sample

      ctx ! evalQuery(
        auth,
        create(
          provider,
          aName.sample,
          roles = Seq("admin", "server", "server-readonly", "client"))) match {
        case Left(errors) =>
          errors shouldBe List(
            ValidationError(
              List(
                ValidationFailure(
                  AccessProvider.RolesField.path,
                  "Builtin role `admin` not allowed."),
                ValidationFailure(
                  AccessProvider.RolesField.path,
                  "Builtin role `server` not allowed."),
                ValidationFailure(
                  AccessProvider.RolesField.path,
                  "Builtin role `server-readonly` not allowed."),
                ValidationFailure(
                  AccessProvider.RolesField.path,
                  "Builtin role `client` not allowed.")
              ),
              RootPosition at "create_access_provider"
            ))

        case Right(_) =>
          fail()
      }
    }

    "access provider already exists" in {
      val provider = aName.sample

      val ref = (ctx ! runQuery(auth, Select("ref", create(provider, aName.sample))))
        .asInstanceOf[RefL]

      ctx ! evalQuery(auth, create(provider, aName.sample)) match {
        case Left(errors) =>
          errors shouldBe List(
            InstanceAlreadyExists(ref.id, RootPosition at "create_access_provider"))
        case Right(_) =>
          fail()
      }
    }

    "issuer cannot be duplicated" in {
      val provider = aName.sample

      ctx ! runQuery(auth, create(provider, "anIssuer"))
      ctx ! runQuery(auth, create(aName.sample, "anotherIssuer"))

      // create with duplicate issuer
      ctx ! evalQuery(auth, create(aName.sample, "anIssuer")) match {
        case Left(errors) =>
          errors shouldBe List(
            ValidationError(
              List(DuplicateValue(AccessProvider.IssuerField.path)),
              RootPosition at "create_access_provider"))
        case Right(_) =>
          fail()
      }

      // update to existent issuer
      ctx ! evalQuery(
        auth,
        Update(
          AccessProviderRef(provider),
          MkObject("issuer" -> "anotherIssuer"))) match {
        case Left(errors) =>
          // fixme: validators are not generating correct positions
          errors shouldBe List(
            ValidationError(
              List(DuplicateValue(AccessProvider.IssuerField.path)),
              RootPosition))
        case Right(_) =>
          fail()
      }
    }

    "jwks_uri should be safe" in {
      ctx ! evalQuery(
        auth,
        create(
          aName.sample,
          aName.sample,
          "http://fauna.auth0.com/.well-known/jwks.json")) match {
        case Left(errors) =>
          errors shouldBe List(
            ValidationError(
              List(
                InvalidURI(
                  AccessProvider.JwksUriField.path,
                  "http://fauna.auth0.com/.well-known/jwks.json")),
              RootPosition at "create_access_provider"))
        case Right(_) =>
          fail()
      }

      ctx ! evalQuery(
        auth,
        create(aName.sample, aName.sample, "db.fauna.com")) match {
        case Left(errors) =>
          errors shouldBe List(
            ValidationError(
              List(InvalidURI(AccessProvider.JwksUriField.path, "db.fauna.com")),
              RootPosition at "create_access_provider"))
        case Right(_) =>
          fail()
      }
    }

    "accept role names" in {
      ctx ! evalQuery(
        auth,
        CreateRole(
          MkObject(
            "name" -> "aRole",
            "privileges" -> MkObject(
              "resource" -> ClassesNativeClassRef,
              "actions" -> MkObject("create" -> true)
            )
          ))
      ) should matchPattern { case Right(_) =>
      }

      ctx ! evalQuery(
        auth,
        create(
          aName.sample,
          aName.sample,
          roles = Seq("aRole"))) should matchPattern { case Right(_) =>
      }

      ctx ! evalQuery(
        auth,
        create(
          aName.sample,
          aName.sample,
          roles = Seq("anotherRole"))) should matchPattern {
        case Left(
              List(
                ValidationError(
                  List(ValidationFailure(
                    AccessProvider.RolesField.path,
                    "Role `anotherRole` does not exist.")),
                  _))) =>
      }
    }

    "renders audience" in {
      val name = aName.sample
      val issuer = aName.sample

      val version = runQuery(auth, create(name, issuer)) map {
        case VersionL(v, _) => v
        case r              => sys.error(s"Unknown result: $r")
      }

      val readable = ReadAdaptor.readableData(ctx ! version)
      readable.fields.contains(List("audience")) shouldBe true
      readable.fields.get(List("audience")).get shouldBe StringV(
        JWTToken.canonicalDBUrl(globalID))
    }

    "allow audience in select/contains_field" in {
      val name = aName.sample
      val issuer = aName.sample

      ctx ! runQuery(auth, create(name, issuer))

      (ctx ! runQuery(
        auth,
        ContainsField("audience", Get(AccessProviderRef(name))))) shouldBe TrueL
      (ctx ! runQuery(
        auth,
        Select("audience", Get(AccessProviderRef(name))))) shouldBe StringL(
        JWTToken.canonicalDBUrl(globalID))
    }

    "disallows the audience to be altered via update" in {
      val name = aName.sample
      val issuer = aName.sample

      ctx ! runQuery(auth, create(name, issuer))

      (ctx ! runQuery(
        auth,
        Select("audience", Get(AccessProviderRef(name))))) shouldBe StringL(
        JWTToken.canonicalDBUrl(globalID))

      ctx ! runQuery(
        auth,
        Update(AccessProviderRef(name), MkObject("audience" -> "ignore")))

      (ctx ! runQuery(
        auth,
        Select("audience", Get(AccessProviderRef(name))))) shouldBe StringL(
        JWTToken.canonicalDBUrl(globalID))
    }

    "disallows the audience to be altered via replace" in {
      val name = aName.sample
      val issuer = aName.sample

      ctx ! runQuery(auth, create(name, issuer))

      (ctx ! runQuery(
        auth,
        Select("audience", Get(AccessProviderRef(name))))) shouldBe StringL(
        JWTToken.canonicalDBUrl(globalID))

      ctx ! runQuery(
        auth,
        Replace(AccessProviderRef(name), MkObject("audience" -> "ignore")))

      (ctx ! runQuery(
        auth,
        Select("audience", Get(AccessProviderRef(name))))) shouldBe StringL(
        JWTToken.canonicalDBUrl(globalID))
    }
  }

  private def patched(map: MapV): MapV = {
    val res = patch(map)
    res.getOrElse(fail()).fields
  }

  private def patchFailures(map: MapV): List[ValidationException] = {
    patch(map) match {
      case Left(v)  => v
      case Right(_) => fail()
    }
  }

  private def patch[A](map: MapV): Either[List[ValidationException], Data] = {
    ctx ! AccessProvider.VersionValidator.patch(
      Data.empty,
      Data.empty diffTo Data(map)
    )
  }
}
