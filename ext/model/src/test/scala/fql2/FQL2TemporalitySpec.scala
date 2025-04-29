package fauna.model.test

import fauna.atoms.DocID
import fauna.auth.Auth
import fauna.lang.Timestamp
import fauna.repo.values.Value
import fql.ast.Span

class FQL2TemporalitySpec extends FQL2Spec {
  var auth: Auth = _
  var aliceId: DocID = _
  var alice: String = ""
  var bobId: DocID = _
  var bob: String = ""
  var ts: Timestamp = _
  var tsStr: String = ""

  before {
    auth = newDB

    evalOk(auth, "Collection.create({ name: 'User' })")
    val res = evalRes(
      auth,
      s"""|let alice = User.create({ name: 'Alice' })
          |let bob = User.create({ name: 'Bob', other: alice })
          |{ alice: alice, bob: bob }
          |""".stripMargin
    )
    aliceId = (res.value / "alice").as[DocID]
    alice = s"User.byId('${aliceId.subID.toLong}')!"
    bobId = (res.value / "bob").as[DocID]
    bob = s"User.byId('${bobId.subID.toLong}')!"
    ts = res.ts
    tsStr = s"Time('$ts')"

    evalOk(
      auth,
      s"""|$alice.update({ name: 'Carol' })
          |$bob.update({ name: 'Joe' })
          |""".stripMargin
    )
  }

  "lookup docs in the past" in {
    evalOk(auth, s"$bob") shouldBe Value.Doc(bobId, None)
    evalOk(auth, s"at ($tsStr) { $bob }") shouldBe Value.Doc(bobId, None, Some(ts))
  }

  "lookup name in the past" in {
    evalOk(auth, s"at ($tsStr) { $bob.name }") shouldBe Value.Str("Bob")
    evalOk(auth, s"(at ($tsStr) { $bob }).name") shouldBe Value.Str("Bob")
  }

  "lookup other docs in the past" in {
    evalOk(auth, s"$bob.other.name") shouldBe Value.Str("Carol")
    evalOk(auth, s"at ($tsStr) { $bob.other.name }") shouldBe Value.Str("Alice")
    evalOk(auth, s"(at ($tsStr) { $bob.other }).name") shouldBe Value.Str("Alice")
    evalOk(auth, s"(at ($tsStr) { $bob }).other.name") shouldBe Value.Str("Alice")
  }

  "deleting a doc should work" in {
    evalOk(auth, s"$alice.delete()")
    evalOk(auth, s"at ($tsStr) { $bob.other.name }") shouldBe Value.Str("Alice")
    evalOk(auth, s"(at ($tsStr) { $bob.other }).name") shouldBe Value.Str("Alice")
    evalOk(auth, s"(at ($tsStr) { $bob }).other.name") shouldBe Value.Str("Alice")

    evalOk(auth, s"$bob.other?.name") shouldBe Value.Null(Span.Null)
  }
}
