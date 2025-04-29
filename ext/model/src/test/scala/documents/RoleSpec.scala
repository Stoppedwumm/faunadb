package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.Auth
import fauna.model._
import fauna.repo.test.CassandraHelper
import fauna.storage.doc._
import fauna.storage.ir._

class RoleSpec extends Spec {

  private val ctx = CassandraHelper.context("model")
  private val anInstance = DocIDV(CollectionID(2024).toDocID)
  private val aPredicate = QueryV("_", true)

  private val aValidRole = MapV(
    "name" -> "simple",
    "membership" -> MapV(
      "resource" -> anInstance,
      "predicate" -> aPredicate
    ),
    "privileges" -> MapV(
      "resource" -> anInstance,
      "actions" -> MapV(
        "read" -> aPredicate,
        "write" -> true,
        "create" -> true,
        "create_with_id" -> true,
        "delete" -> true,
        "history_write" -> true,
        "history_read" -> true,
        "unrestricted_read" -> true,
        "call" -> true
      )
    )
  )

  private val roleWithInvalidFields = aValidRole
    .update(List("foo"), "bar")
    .update(List("membership", "foo"), "bar")
    .update(List("privileges", "foo"), "bar")
    .update(List("privileges", "actions", "foo"), "bar")

  "RoleVersionValidatorSpec" - {
    "requires name and privileges" in {
      patchFailures(MapV.empty) should equal(
        List(
          ValueRequired(List("name"))
        ))
    }

    "filter out invalid fields" in {
      patched(roleWithInvalidFields) should equal(aValidRole)
    }

    "filter out invalid fields on array elements" in {
      def wrapped(map: MapV): MapV = {
        List("membership", "privileges").foldLeft(map) {
          case (m, field) =>
            m.update(List(field), ArrayV(m.get(List(field)).get))
        }
      }

      patched(wrapped(roleWithInvalidFields)) should equal(wrapped(aValidRole))
    }

    "fail on wrong role action type" in {
      patchFailures(
        aValidRole
          .update(List("privileges", "actions", "create"), 42)
      ).head.code should equal("invalid type")
    }

    "fail on invalid arity" in {
      def checkArity(action: String, expected: Int) = {
        val actonPath = List("privileges", "actions", action)
        val invalidArity = expected + 1
        val args = (0 until invalidArity) map { n =>
          StringV(s"p$n")
        }

        patchFailures(
          aValidRole
            .update(
              actonPath,
              QueryV(ArrayV(args.toVector), true)
            )
        ) should contain only
          InvalidArity(actonPath, s"$expected", s"$invalidArity")
      }

      checkArity("read", 1)
      checkArity("write", 2)
      checkArity("create", 1)
      checkArity("create_with_id", 2)
      checkArity("delete", 1)
      checkArity("history_read", 1)
      checkArity("history_write", 4)
      checkArity("unrestricted_read", 1)
      checkArity("call", 1)
    }

    "allow to ignore predicate arguments" in {
      def canIgnorePredicateArgs(action: String) = {
        patch(
          aValidRole
            .update(
              List("privileges", "actions", action),
              QueryV("_", true)
            )
        ).isRight should be(true)
      }

      canIgnorePredicateArgs("read")
      canIgnorePredicateArgs("write")
      canIgnorePredicateArgs("create")
      canIgnorePredicateArgs("create_with_id")
      canIgnorePredicateArgs("delete")
      canIgnorePredicateArgs("history_read")
      canIgnorePredicateArgs("history_write")
      canIgnorePredicateArgs("unrestricted_read")
      canIgnorePredicateArgs("call")
    }

    "fail on invalid membership arity" in {
      patchFailures(
        aValidRole
          .update(
            List("membership", "predicate"),
            QueryV(ArrayV("foo", "bar"), true)
          )
      ) should contain only
        InvalidArity(List("membership", "predicate"), "1", "2")
    }
  }

  "RoleSpec" - {
    import SocialHelpers._

    val ctx = CassandraHelper.context("model")

    val scopeID = ctx ! newScope
    val auth = Auth.adminForScope(scopeID)

    "role already exists" in {
      ctx ! mkCollection(auth, MkObject("name" -> "someclass"))

      val aPredicate = QueryF(Lambda("x" -> true))

      val create = CreateRole(
        MkObject(
          "name" -> "somerole",
          "membership" -> MkObject(
            "resource" -> ClassRef("someclass"),
            "predicate" -> aPredicate
          ),
          "privileges" -> MkObject(
            "resource" -> ClassRef("someclass"),
            "actions" -> MkObject(
              "read" -> aPredicate,
              "write" -> true,
              "create" -> true,
              "create_with_id" -> true,
              "delete" -> true,
              "history_write" -> true,
              "history_read" -> true,
              "unrestricted_read" -> true,
              "call" -> true
            )
          )
        ))

      val ref = (ctx ! runQuery(auth, Select("ref", create))).asInstanceOf[RefL]
      (ctx ! evalQuery(auth, create)) match {
        case Left(errors) =>
          errors shouldBe List(InstanceAlreadyExists(ref.id, RootPosition at "create_role"))
        case Right(_) => fail()
      }
    }
  }

  private def patched(map: MapV): MapV = {
    val res = patch(map)
    res.getOrElse(fail()).fields
  }

  private def patchFailures(map: MapV): List[ValidationException] = {
    patch(map) match {
      case Left(v) => v
      case Right(_) => fail()
    }
  }

  private def patch[A](map: MapV): Either[List[ValidationException], Data] = {
    ctx ! Role.VersionValidator.patch(
      Data.empty,
      Data.empty diffTo Data(map)
    )
  }
}
