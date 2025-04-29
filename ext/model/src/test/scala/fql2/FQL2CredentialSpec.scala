package fauna.model.test

import fauna.atoms.{ CredentialsID, DocID }
import fauna.auth.{ AdminPermissions, Auth }
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.repo.schema.ConstraintFailure.ValidatorFailure
import fauna.repo.values.Value
import fauna.util.BCrypt
import org.scalatest.tags.Slow
import scala.concurrent.duration._

@Slow
class FQL2CredentialSpec extends FQL2Spec {

  def login(auth: Auth, doc: DocID, coll: String, passwd: String) = {
    val token = evalOk(
      auth,
      s"""|let doc = $coll.byId('${doc.subID.toLong}')!
          |let cred = Credentials.byDocument(doc)!
          |cred.login('$passwd')""".stripMargin
    )

    (getDocFields(auth, token) / "secret").as[String]
  }

  "FQL2CredentialSpec" - {
    "create" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val doc = evalOk(auth, """Person.create({})""").as[DocID]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Person.byId('${doc.subID.toLong}'),
              |  password: "sekret"
              |})""".stripMargin
        ) should matchPattern { case Value.Doc(_, _, _, _, _) => }

        val token = login(auth, doc, "Person", "sekret")

        evalOk(token, "'ok'") shouldBe Value.Str("ok")
      }

      "cannot create a Credential with a native document" in {
        val auth = newDB.withPermissions(AdminPermissions)
        val id =
          evalOk(auth, "Key.create({ role: 'server' }).id").asInstanceOf[Value.ID]
        val res = evalErr(
          auth,
          s"""|Credentials.create({
              |  document: Key.byId('${id.value}'),
              |  password: '123'
              |})""".stripMargin
        )

        inside(res) {
          case QueryRuntimeFailure("constraint_failure", _, _, _, Seq(cf), _) =>
            val ValidatorFailure(path, msg) = cf
            path.toString shouldEqual "document"
            msg shouldEqual "Expected document from a user-defined collection."
        }
      }
    }

    "update" - {
      "password" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val doc = evalOk(auth, """Person.create({})""").as[DocID]

        evalOk(
          auth,
          s"""|Credentials.create({
              |  document: Person.byId('${doc.subID.toLong}'),
              |  password: "sekret"
              |})""".stripMargin
        ) should matchPattern { case Value.Doc(_, _, _, _, _) => }

        evalOk(
          auth,
          """|Credentials.all().first()!.update({
             |  password: 'new-sekret'
             |})""".stripMargin
        )

        val token = login(auth, doc, "Person", "new-sekret")

        evalOk(token, "'ok'") shouldBe Value.Str("ok")
      }

      "document" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val doc0 = evalOk(auth, """Person.create({})""").as[DocID]
        val doc1 = evalOk(auth, """Person.create({})""").as[DocID]

        evalOk(
          auth,
          s"""|Credentials.create({
              |  document: Person.byId('${doc0.subID.toLong}'),
              |  password: "sekret"
              |})""".stripMargin
        ) should matchPattern { case Value.Doc(_, _, _, _, _) => }

        evalOk(
          auth,
          s"""|Credentials.all().first()!.update({
              |  document: Person.byId('${doc1.subID.toLong}'),
              |})""".stripMargin
        )

        val token = login(auth, doc1, "Person", "sekret")

        evalOk(token, "'ok'") shouldBe Value.Str("ok")
      }
    }

    "replace" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val doc0 = evalOk(auth, """Person.create({})""").as[DocID]
        val doc1 = evalOk(auth, """Person.create({})""").as[DocID]

        evalOk(
          auth,
          s"""|Credentials.create({
              |  document: Person.byId('${doc0.subID.toLong}'),
              |  password: "sekret"
              |})""".stripMargin
        ) should matchPattern { case Value.Doc(_, _, _, _, _) => }

        evalOk(
          auth,
          s"""|Credentials.all().first()!.replace({
              |  document: Person.byId('${doc1.subID.toLong}'),
              |  password: "new-sekret"
              |})""".stripMargin
        )

        val token = login(auth, doc1, "Person", "new-sekret")

        evalOk(token, "'ok'") shouldBe Value.Str("ok")
      }
    }

    "delete" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val doc = evalOk(auth, """Person.create({})""").as[DocID]

        evalOk(
          auth,
          s"""|Credentials.create({
              |  document: Person.byId('${doc.subID.toLong}'),
              |  password: "sekret"
              |})""".stripMargin
        ) should matchPattern { case Value.Doc(_, _, _, _, _) => }

        evalOk(login(auth, doc, "Person", "sekret"), "'ok'") shouldBe Value.Str("ok")

        evalOk(auth, """Credentials.all().first()!.delete()""")

        an[Exception] should be thrownBy {
          login(auth, doc, "Person", "sekret")
        }
      }
    }

    "byDocument" in {
      val auth = newDB

      evalOk(auth, """Collection.create({name: 'Person'})""")

      val doc0 = evalOk(auth, """Person.create({}).id""").as[Long]
      val doc1 = evalOk(auth, """Person.create({}).id""").as[Long]

      val cred = evalOk(
        auth,
        s"""|Credentials.create({
            |  document: Person.byId('$doc0'),
            |  password: "sekret"
            |})""".stripMargin
      )

      evalOk(
        auth,
        s"""|let document = Person.byId('$doc0')
            |Credentials.byDocument(document)""".stripMargin
      ) shouldBe cred

      evalOk(
        auth,
        s"""|let document: Person | NullPerson = Person.byId('$doc0')
            |Credentials.byDocument(document)""".stripMargin
      ) shouldBe cred

      evalOk(
        auth,
        s"""|let document: Ref<Person> = Person.byId('$doc0')
            |Credentials.byDocument(document)""".stripMargin
      ) shouldBe cred

      evalOk(
        auth,
        s"Credential.byDocument(Person.byId('$doc1')).exists()") shouldEqual Value.False
      evalOk(
        auth,
        s"Credential.byDocument(Person.byId('$doc1')) == null") shouldEqual Value.True
    }

    "verify" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val cred = evalOk(
          auth,
          s"""|let person = Person.create({})
              |
              |Credentials.create({
              |  document: person,
              |  password: "sekret"
              |}).id""".stripMargin
        ).as[Long]

        evalOk(auth, s"""Credentials.byId('$cred')!.verify('sekret')""")
          .as[Boolean] shouldBe true

        evalOk(auth, s"""Credential.byId('$cred')!.verify('invalid')""")
          .as[Boolean] shouldBe false
      }

      def permissionTest(resource: String) = {
        val auth = newDB

        val key = evalOk(
          auth.withPermissions(AdminPermissions),
          s"""|Role.create({
             |  name: "creds",
             |  privileges: {
             |    resource: "${resource}",
             |    actions: {
             |      read: false
             |    }
             |  }
             |})
             |
             |Key.create({
             |  role: "creds"
             |})""".stripMargin
        )

        evalOk(auth, """Collection.create({name: 'Person'})""")

        val cred = evalOk(
          auth,
          s"""|let person = Person.create({})
              |
              |Credential.create({
              |  document: person,
              |  password: "sekret"
              |}).id""".stripMargin
        ).as[Long]

        evalOk(
          (getDocFields(auth, key) / "secret").as[String],
          s"""Credential.byId('$cred')?.verify('sekret')"""
        ) should matchPattern {
          case Value.Null(
                Value.Null.Cause
                  .ReadPermissionDenied(DocID(_, CredentialsID.collID), _)) =>
        }
      }

      "respect permissions" in {
        behave like permissionTest("Credential")

        // Test the alias.
        behave like permissionTest("Credentials")
      }
    }

    "login" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Users'})""")

        val user = evalOk(auth, """Users.create({}).id""").as[Long]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Users.byId('$user'),
              |  password: "sekret"
              |})""".stripMargin
        )

        val token = evalOk(
          auth,
          s"""|let user = Users.byId('$user')
              |let cred = Credential.byDocument(user)!
              |cred.login("sekret")
              |""".stripMargin
        )

        val fields = getDocFields(auth, token)
        val secret = (fields / "secret").as[String]

        evalOk(secret, "1") shouldBe Value.Int(1)
      }

      "login + ttl" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Users'})""")

        val user = evalOk(auth, """Users.create({}).id""").as[Long]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Users.byId('$user'),
              |  password: "sekret"
              |})""".stripMargin
        )

        val token = evalOk(
          auth,
          s"""|let user = Users.byId('$user')
              |let cred = Credential.byDocument(user)!
              |cred.login("sekret", Time.now().add(5, "seconds"))
              |""".stripMargin
        )

        val fields = getDocFields(auth, token)
        val secret = (fields / "secret").as[String]

        evalOk(secret, "1") shouldBe Value.Int(1)

        eventually(timeout(10.seconds)) {
          (ctx ! Auth.lookup(secret)) shouldBe empty
        }
      }

      "ttl now should fail" in pendingUntilFixed {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Users'})""")

        val user = evalOk(auth, """Users.create({}).id""").as[Long]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Users.byId('$user'),
              |  password: "sekret"
              |})""".stripMargin
        )

        evalErr(
          auth,
          s"""|let user = Users.byId('$user')
              |let cred = Credential.byDocument(user)!
              |cred.login("sekret", Time.now())
              |""".stripMargin
        ) should matchPattern { case "fix pattern matching" => }
      }

      "fails with invalid secret" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: 'Users'})""")

        val user = evalOk(auth, """Users.create({}).id""").as[Long]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Users.byId('$user'),
              |  password: "sekret"
              |})""".stripMargin
        )

        evalErr(
          auth,
          s"""|let user = Users.byId('$user')
              |let cred = Credential.byDocument(user)!
              |cred.login("invalid")
              |""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure("invalid_secret", _, _, _, _, _) =>
        }
      }

      "respect permissions" in {
        val auth = newDB

        val key = evalOk(
          auth.withPermissions(AdminPermissions),
          """|Collection.create({name: "Users"})
             |
             |Role.create({
             |  name: "creds",
             |  privileges: [{
             |    resource: "Credential",
             |    actions: {
             |      read: false
             |    }
             |  }, {
             |    resource: "Users",
             |    actions: {
             |      read: true
             |    }
             |  }]
             |})
             |
             |Key.create({
             |  role: "creds"
             |})""".stripMargin
        )

        val user = evalOk(auth, """Users.create({}).id""").as[Long]

        evalOk(
          auth,
          s"""|Credential.create({
              |  document: Users.byId('$user'),
              |  password: "sekret"
              |})""".stripMargin
        )

        // FIXME: unfortunately we can't use the `!` on `byDocument`,
        // as it will check for read permission (which we don't have). Not sure
        // how to improve this, but I think `byDocument` might need a bit of
        // rethinking.
        val auth1 = (ctx ! Auth.fromAuth(
          (getDocFields(auth, key) / "secret").as[String],
          List(BCrypt.hash("secret")))).value
        evalOk(
          auth1,
          s"""|let user = Users.byId('$user')
              |let cred = Credential.byDocument(user)
              |cred?.login("sekret")
              |""".stripMargin,
          typecheck = false
        ) should matchPattern {
          case Value.Null(Value.Null.Cause
                .ReadPermissionDenied(DocID(_, CredentialsID.collID), _)) =>
        }
      }
    }

    "alias" - {
      "works" in {
        val auth = newDB
        evalOk(
          auth,
          "Credentials.all()"
        )
      }
    }
  }
}
