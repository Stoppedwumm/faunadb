package fauna.model.test

import fauna.repo.query.ReadCache
import fauna.repo.query.ReadCache.Fragment
import fauna.repo.values.Value

class FQL2PartialStructSpec extends FQL2Spec {
  "FQL2PartialStructSpec" - {
    "returns a covered value from a struct" in {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'foo.name' }]
           |    }
           |  }
           |})
           |
           |""".stripMargin
      )

      evalOk(auth, "User.create({ foo: { name: 'Alice' } })")

      val p0 = evalOk(auth, "User.names().map(.data).first()!")
        .asInstanceOf[Value.Struct.Partial]
      p0.prefix shouldBe ReadCache.Prefix.empty
      p0.path shouldBe List("data")
      p0.fragment
        .project(List("foo", "name"))
        .get
        .asInstanceOf[Fragment.Value]
        .unwrap shouldBe Value.Str("Alice")

      val p1 = evalOk(auth, "User.names().map(.data.foo).first()!")
        .asInstanceOf[Value.Struct.Partial]
      p1.prefix shouldBe ReadCache.Prefix.empty
      p1.path shouldBe List("data", "foo")
      p1.fragment
        .project(List("name"))
        .get
        .asInstanceOf[Fragment.Value]
        .unwrap shouldBe Value.Str("Alice")

      evalOk(auth, "User.names().map(.data.foo.name).first()!") shouldBe Value.Str(
        "Alice")

      val p2 = evalOk(auth, "User.names().map(.foo).first()!")
        .asInstanceOf[Value.Struct.Partial]
      p2.prefix shouldBe ReadCache.Prefix.empty
      p2.path shouldBe List("data", "foo")
      p1.fragment
        .project(List("name"))
        .get
        .asInstanceOf[Fragment.Value]
        .unwrap shouldBe Value.Str("Alice")

      evalOk(auth, "User.names().map(.foo.name).first()!") shouldBe Value.Str(
        "Alice")
    }

    "returns a covered value before it gets updated" in {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'foo.name' }]
           |    }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "User.create({ foo: { name: 'Alice' } })")

      val res = evalOk(
        auth,
        """|let before = User.names().map(.data.foo).first()!
           |
           |User.all().first()!.update({ foo: { name: 'Bob' } })
           |
           |{
           |  before: before.name,
           |  after: User.names().map(.data.foo).first()!.name,
           |}
           |""".stripMargin
      )

      (res / "before") shouldBe Value.Str("Alice")
      (res / "after") shouldBe Value.Str("Bob")
    }

    "returns a covered value after it gets updated" in {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'foo.name' }]
           |    }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "User.create({ foo: { name: 'Alice' } })")

      // applies one pending write, reads a partial, then applies a second write.
      val res = evalOk(
        auth,
        """|let before = User.names().map(.data.foo).first()!
           |
           |User.all().first()!.update({ foo: { name: 'Bob' } })
           |
           |let middle = User.names().map(.data.foo).first()!
           |
           |User.all().first()!.update({ foo: { name: 'Carol' } })
           |
           |{
           |  before: before.name,
           |  middle: middle.name,
           |  after: User.names().map(.data.foo).first()!.name,
           |}
           |""".stripMargin
      )

      (res / "before") shouldBe Value.Str("Alice")
      (res / "middle") shouldBe Value.Str("Bob")
      (res / "after") shouldBe Value.Str("Carol")
    }

    "works with bracket access" in {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'foo.name' }]
           |    }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "User.create({ foo: { name: 'Alice' } })")

      val p = evalOk(auth, "User.names().map(.data['foo']).first()!")
        .asInstanceOf[Value.Struct.Partial]
      p.prefix shouldBe ReadCache.Prefix.empty
      p.path shouldBe List("data", "foo")
      p.fragment
        .project(List("name"))
        .get
        .asInstanceOf[Fragment.Value]
        .unwrap shouldBe Value.Str("Alice")
    }

    "works with projection" in {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'foo.name' }]
           |    }
           |  }
           |})
           |""".stripMargin
      )

      evalOk(auth, "User.create({ foo: { name: 'Alice' } })")

      val p = (evalOk(auth, "(User.names() { foo }).first()") / "foo")
        .asInstanceOf[Value.Struct.Partial]
      p.prefix shouldBe ReadCache.Prefix.empty
      p.path shouldBe List("data", "foo")
      p.fragment
        .project(List("name"))
        .get
        .asInstanceOf[Fragment.Value]
        .unwrap shouldBe Value.Str("Alice")
    }
  }
}
