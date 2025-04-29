package fauna.model.test

import fauna.atoms.{ DocID, TokenID }
import fauna.auth._
import fauna.model.runtime.fql2.stdlib.TokenCompanion
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.repo.schema.ConstraintFailure.ValidatorFailure
import fauna.repo.values.Value
import scala.concurrent.duration._

class FQL2TokenSpec extends FQL2Spec {

  "FQL2TokenSpec" - {
    "create" - {
      "client cannot create" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        evalErr(
          auth.withPermissions(NullPermissions),
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }

        evalErr(
          auth.withPermissions(ServerReadOnlyPermissions),
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "permission_denied",
                "Insufficient privileges to perform the action.",
                _,
                Seq()) =>
        }
      }

      "server & admin can create" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        def assertCreateWithPerm(permission: Permissions) = {
          val token = evalOk(
            auth.withPermissions(permission),
            s"""|Token.create({
                |  document: Foo.byId('${doc.subID.toLong}'),
                |  ttl: Time.now().add(1, "day"),
                |  data: {
                |    foo: 'bar'
                |  }
                |})""".stripMargin
          )

          val fields = getDocFields(auth, token)

          (fields / "id") should matchPattern { case Value.ID(_) => }
          (fields / "coll") shouldEqual TokenCompanion
          (fields / "secret") should matchPattern { case Value.Str(_) => }
          (fields / "hashed_secret") should matchPattern { case Value.Null(_) => }
          (fields / "ttl") should matchPattern { case Value.Time(_) => }
          (fields / "data" / "foo") should matchPattern { case Value.Str("bar") => }

          evalOk((fields / "secret").as[String], "1") shouldBe Value.Int(1)
        }

        assertCreateWithPerm(ServerPermissions)
        assertCreateWithPerm(AdminPermissions)
      }

      "cannot pass secret" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        // typechecking disallows this, need to check both
        renderErr(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}'),
              |  secret: 'sekert'
              |})""".stripMargin,
          typecheck = false
        ) shouldBe (
          s"""|error: Failed to create Token.
              |constraint failures:
              |  secret: Failed to update field because it is readonly
              |at *query*:1:13
              |  |
              |1 |   Token.create({
              |  |  _____________^
              |2 | |   document: Foo.byId('${doc.subID.toLong}'),
              |3 | |   secret: 'sekert'
              |4 | | })
              |  | |__^
              |  |""".stripMargin
        )

        val err = evalErr(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}'),
              |  secret: 'sekert'
              |})""".stripMargin
        )
        err.errors.head.renderWithSource(
          Map.empty) shouldBe (s"""|error: Type `{ document: Ref<Foo>, secret: "sekert" }` contains extra field `secret`
             |at *query*:1:14
             |  |
             |1 |   Token.create({
             |  |  ______________^
             |2 | |   document: Foo.byId('${doc.subID.toLong}'),
             |3 | |   secret: 'sekert'
             |4 | | })
             |  | |_^
             |  |""".stripMargin)
      }

      "secret is ephemeral" in {
        val auth = newDB
        val admin = auth.withPermissions(AdminPermissions)

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val id = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |}).id""".stripMargin
        ).as[Long]

        evalOk(admin, s"""Token.byId('$id')!.secret""") should matchPattern {
          case Value.Null(_) =>
        }
      }

      "cannot create a Token with a native document" in {
        val auth = newDB.withPermissions(AdminPermissions)
        val id =
          evalOk(auth, "Key.create({ role: 'server' }).id").as[Long]
        val res = evalErr(
          auth,
          s"""|Token.create({
              |  document: Key.byId('$id')
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
      "cannot update secret" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        )

        val id = (getDocFields(auth, token) / "id").as[Long]

        renderErr(
          auth,
          s"""|Token.byId('$id')!.update({
              |  secret: 'sekret'
              |})""".stripMargin
        ) shouldBe (
          s"""|error: Failed to update Token with id $id.
              |constraint failures:
              |  secret: Failed to update field because it is readonly
              |at *query*:1:41
              |  |
              |1 |   Token.byId('$id')!.update({
              |  |  _________________________________________^
              |2 | |   secret: 'sekret'
              |3 | | })
              |  | |__^
              |  |""".stripMargin
        )
      }

      "can change document" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc0 = evalOk(auth, """Foo.create({})""").as[DocID]
        val doc1 = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc0.subID.toLong}')
              |})""".stripMargin
        )
        val fields = getDocFields(auth, token)

        val id = (fields / "id").as[Long]
        val secret = (fields / "secret").as[String]

        evalOk(
          auth,
          s"""|Token.byId('$id')!.update({
              |  document: Foo.byId('${doc1.subID.toLong}')
              |})""".stripMargin
        )

        val updated = evalOk(auth, s"""Token.byId('$id') { document }""")

        (updated / "document") should matchPattern {
          case Value.Doc(`doc1`, _, _, _, _) =>
        }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }

      "can add data" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        )
        val fields = getDocFields(auth, token)

        val id = (fields / "id").as[Long]
        val secret = (fields / "secret").as[String]

        evalOk(
          auth,
          s"""|Token.byId('$id')!.update({
              |  data: {
              |    foo: 'bar'
              |  }
              |})""".stripMargin
        )

        val updated = evalOk(auth, s"""Token.byId('$id') { data }""")

        (updated / "data" / "foo") should matchPattern { case Value.Str("bar") => }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }
    }

    "replace" - {
      "cannot replace secret" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val id = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |}).id""".stripMargin
        ).as[Long]

        renderErr(
          auth,
          s"""|Token.byId('$id')!.replace({
              |  document: Foo.byId('${doc.subID.toLong}'),
              |  secret: 'sekret'
              |})""".stripMargin
        ) shouldBe (
          s"""|error: Failed to update Token with id $id.
              |constraint failures:
              |  secret: Failed to update field because it is readonly
              |at *query*:1:42
              |  |
              |1 |   Token.byId('$id')!.replace({
              |  |  __________________________________________^
              |2 | |   document: Foo.byId('${doc.subID.toLong}'),
              |3 | |   secret: 'sekret'
              |4 | | })
              |  | |__^
              |  |""".stripMargin
        )
      }

      "can change document" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc0 = evalOk(auth, """Foo.create({})""").as[DocID]
        val doc1 = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc0.subID.toLong}')
              |})""".stripMargin
        )
        val fields = getDocFields(auth, token)

        val id = (fields / "id").as[Long]
        val secret = (fields / "secret").as[String]

        evalOk(
          auth,
          s"""|Token.byId('$id')!.replace({
              |  document: Foo.byId('${doc1.subID.toLong}')
              |})""".stripMargin
        )

        val updated = evalOk(auth, s"""Token.byId('$id') { document }""")

        (updated / "document") should matchPattern {
          case Value.Doc(`doc1`, _, _, _, _) =>
        }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }

      "can add data" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        )
        val fields = getDocFields(auth, token)

        val id = (fields / "id").as[Long]
        val secret = (fields / "secret").as[String]

        evalOk(
          auth,
          s"""|Token.byId('$id')!.replace({
              |  document: Foo.byId('${doc.subID.toLong}'),
              |  data: {
              |    foo: 'bar'
              |  }
              |})""".stripMargin
        )

        val updated = evalOk(auth, s"""Token.byId('$id') { data }""")

        (updated / "data" / "foo") should matchPattern { case Value.Str("bar") => }

        evalOk(secret, "1") shouldBe Value.Int(1)
      }
    }

    "delete" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc = evalOk(auth, """Foo.create({})""").as[DocID]

        val token = evalOk(
          auth,
          s"""|Token.create({
              |  document: Foo.byId('${doc.subID.toLong}')
              |})""".stripMargin
        )
        val fields = getDocFields(auth, token)

        val id = (fields / "id").as[Long]
        val secret = (fields / "secret").as[String]

        evalOk(secret, "1") shouldBe Value.Int(1)

        evalOk(auth, s"""Token.byId('$id')!.delete()""")

        eventually(timeout(5.seconds)) {
          (ctx ! Auth.lookup(secret)) shouldBe empty
        }
      }
    }

    "byDocument" - {
      "works" in {
        val auth = newDB

        evalOk(auth, """Collection.create({name: "Foo"})""")

        val doc0 = evalOk(auth, """Foo.create({}).id""").as[Long]
        val doc1 = evalOk(auth, """Foo.create({}).id""").as[Long]
        val doc2 = evalOk(auth, """Foo.create({}).id""").as[Long]

        val doc1tokens =
          evalOk(auth, s"""[ Token.create({document: Foo.byId('$doc1')}).id ]""")
            .as[Vector[Long]]
            .map(TokenID(_).toDocID)

        val doc2tokens =
          evalOk(
            auth,
            s"""|[
                |  Token.create({document: Foo.byId('$doc2')}).id,
                |  Token.create({document: Foo.byId('$doc2')}).id
                |]""".stripMargin
          ).as[Vector[Long]].map(TokenID(_).toDocID)

        evalOk(auth, s"""Token.byDocument(Foo.byId('$doc0')!).toArray()""")
          .as[Vector[DocID]] shouldBe empty

        evalOk(auth, s"""Token.byDocument(Foo.byId('$doc1')!).toArray()""")
          .as[Vector[DocID]] should contain allElementsOf doc1tokens

        evalOk(auth, s"""Token.byDocument(Foo.byId('$doc2')!).toArray()""")
          .as[Vector[DocID]] should contain allElementsOf doc2tokens
      }
    }
  }
}

class FQL2TokenWithV4Spec extends FQL2WithV4Spec {
  "logout with a non-existent token doesn't 500" in {
    val auth = newDB

    updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")

    val doc = evalOk(auth, """Foo.create({})""").as[DocID].subID.toLong

    val secret =
      evalOk(auth, s"Token.create({ id: 3, document: Foo($doc) }).secret").as[String]
    val auth2 = ctx ! Auth.lookup(secret).map(_.get)

    // Logout works.
    evalV4Ok(auth2, Logout(false))
    evalOk(auth, "if (Token.all().count() != 0) abort(0)")

    // This shouldn't blow up, it should just do nothing (now that the token is
    // deleted).
    evalV4Ok(auth2, Logout(false))
  }

  "logout twice shouldn't 500" in {
    val auth = newDB

    updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")

    val doc = evalOk(auth, """Foo.create({})""").as[DocID].subID.toLong

    val secret =
      evalOk(auth, s"Token.create({ id: 3, document: Foo($doc) }).secret").as[String]
    val auth2 = ctx ! Auth.lookup(secret).map(_.get)

    evalV4Ok(auth2, Do(Logout(false), Logout(false)))
    evalOk(auth, "if (Token.all().count() != 0) abort(0)")
  }
}
