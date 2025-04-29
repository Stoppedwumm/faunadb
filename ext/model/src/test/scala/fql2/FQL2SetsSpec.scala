package fauna.model.test

import fauna.auth.{ AdminPermissions, Auth, RootAuth, SystemAuth }
import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.{
  FQLInterpreter,
  IndexSet,
  QueryRuntimeFailure,
  Result,
  ValueSet
}
import fauna.model.schema.index.CollectionIndex
import fauna.repo.values.{ ExprCBORCodecs, ExprCBORCodecsNoSpan, Value }
import fauna.repo.values.Value.SetCursor
import fauna.storage.ir.IRValue
import fauna.util.Base64
import fql.ast.{ Expr, Span }
import org.scalactic.source.Position
import org.scalatest.tags.Slow
import scala.collection.immutable.ArraySeq

@Slow
class FQL2SetsSpec extends FQL2Spec {

  def mkDocs(auth: Auth, collname: String, count: Int): ArraySeq[Value.Doc] = {
    val creates =
      (1 to count)
        .map { i =>
          val side = if (i % 2 == 0) "heads" else "tails"
          s"""$collname.create({ foo: $i, flip: "$side" })"""
        }
        .mkString("\n")

    val res =
      evalOk(
        auth,
        s"""|$creates
            |$collname.all().paginate($count).data
            |""".stripMargin
      ).to[Value.Array]

    res.elems.map(_.to[Value.Doc])
  }

  var auth: Auth = _
  var docs: ArraySeq[Value.Doc] = _
  var singleton: Value = _

  before {
    auth = newDB
    mkColl(auth, "User")
    docs = mkDocs(auth, "User", 100)
    singleton = evalOk(auth, "User")
  }

  "FQL2Sets" - {
    ".all returns a set" in {
      val res = evalOk(auth, "User.all()").to[IndexSet]
      res.parent shouldEqual singleton
      res.name shouldEqual "all"
      res.args shouldEqual Vector()
    }

    "user index should read from underlying document" in {
      evalOk(
        auth,
        """|User.definition.update({
           |  indexes: {
           |    byName: {
           |      terms: [{ field: "foo" }]
           |    }
           |  }
           |})""".stripMargin
      )

      val res = evalOk(auth, "User.byName(1)").to[IndexSet]
      res.parent shouldEqual singleton
      res.name shouldEqual "byName"
      res.args shouldEqual Vector(Value.Number(1))
    }

    ".paginate() returns a page struct" in {
      val res = evalOk(auth, "User.all().paginate()")

      res should matchPattern {
        case Value.Struct.Full(fields, _, _, _)
            if fields.contains("data") && fields.contains("after") =>
      }

      val page = res.asInstanceOf[Value.Struct.Full]

      page.fields("data") shouldEqual Value.Array(docs.take(16))
      page.fields("after") should matchPattern {
        case Value.SetCursor(_, _, _, None, 16) =>
      }
    }

    "sets can project" in {
      val res = evalOk(
        auth,
        """|let set = User.all() { foo }
           |set.take(5).toArray()""".stripMargin
      )
      res shouldEqual {
        (1 to 5)
          .map(i => Value.Struct("foo" -> Value.Int(i)))
          .to(Value.Array)
      }
    }

    "sets can encode and decode tokens with and without spans" in {
      val spanCodec = {
        import ExprCBORCodecs.exprs
        CBOR
          .TupleCodec[(
            Expr,
            Vector[IRValue],
            Option[Vector[IRValue]],
            Option[Timestamp],
            scala.Int)]
      }
      val spanlessCodec = {
        import ExprCBORCodecsNoSpan.exprs
        CBOR
          .TupleCodec[(
            Expr,
            Vector[IRValue],
            Option[Vector[IRValue]],
            Option[Timestamp],
            scala.Int)]
      }

      val first = evalOk(auth, "[1, 2].toSet().concat([3, 4].toSet()).paginate(2)")
      val page = first.asInstanceOf[Value.Struct.Full]
      val cursor = page.fields("after")
      val SetCursor(set, values, ords, stOpt, psize) = cursor
      val vctx = values.map(Value.toIR(_).toOption.get)
      val buf = CBOR.encode((set, vctx, ords, stOpt, psize))(spanCodec)
      val encodedSpanCursor = Base64.encodeStandardAscii(buf.nioBuffer)
      val nextWithSpan = evalPage(auth, encodedSpanCursor.toString)
      (nextWithSpan / "data") shouldEqual Value.Array(Value.Int(3), Value.Int(4))

      val spanlessBuf = CBOR.encode((set, vctx, ords, stOpt, psize))(spanlessCodec)
      val encodedSpanlessCursor = Base64.encodeStandardAscii(spanlessBuf.nioBuffer)
      val nextWithoutSpan = evalPage(auth, encodedSpanlessCursor.toString)
      (nextWithoutSpan / "data") shouldEqual Value.Array(Value.Int(3), Value.Int(4))
    }

    ".concat() paginates correctly" in {
      val first = evalOk(auth, "[1, 2].toSet().concat([3, 4].toSet()).paginate(2)")
      (first / "data") shouldEqual Value.Array(Value.Int(1), Value.Int(2))

      val next = evalPage(auth, first / "after")
      (next / "data") shouldEqual Value.Array(Value.Int(3), Value.Int(4))
      (next / "after") shouldEqual Value.Null(Span.Null)

      val first2 =
        evalOk(auth, s"User.all().concat(User.all()).paginate(${docs.size})")
      (first2 / "data") shouldEqual Value.Array(docs)

      val next2 = evalPage(auth, first2 / "after")
      (next2 / "data") shouldEqual Value.Array(docs)
      (next2 / "after") shouldEqual Value.Null(Span.Null)

      val first3 = evalOk(
        auth,
        s"User.all().concat(User.all().reverse()).paginate(${docs.size})")
      (first3 / "data") shouldEqual Value.Array(docs)

      val next3 = evalPage(auth, first3 / "after")
      (next3 / "data") shouldEqual Value.Array(docs.reverse)
      (next3 / "after") shouldEqual Value.Null(Span.Null)

      val first4 =
        evalOk(auth, "[1, 2].toSet().concat([4, 3].toSet().order()).paginate(2)")
      (first4 / "data") shouldEqual Value.Array(Value.Int(1), Value.Int(2))

      val next4 = evalPage(auth, first4 / "after")
      (next4 / "data") shouldEqual Value.Array(Value.Int(3), Value.Int(4))
      (next4 / "after") shouldEqual Value.Null(Span.Null)
    }

    ".concat() handles empty pages" in {
      val first = evalOk(auth, "[1, 2].toSet().concat([].toSet()).paginate(2)")
      (first / "data") shouldEqual Value.Array(Value.Int(1), Value.Int(2))

      val next = evalPage(auth, first / "after")
      (next / "data") shouldEqual Value.Array()
      (next / "after") shouldEqual Value.Null(Span.Null)
    }

    ".concat() handles non-boundry page sizes" in {
      val first = evalOk(auth, "[1, 2, 3].toSet().concat([4].toSet()).paginate(2)")
      (first / "data") shouldEqual Value.Array(Value.Int(1), Value.Int(2))

      val next = evalPage(auth, first / "after")
      (next / "data") shouldEqual Value.Array(Value.Int(3), Value.Int(4))
      (next / "after") shouldEqual Value.Null(Span.Null)

      val first2 = evalOk(auth, s"User.all().concat(User.all()).paginate(2)")
      (first2 / "data") shouldEqual Value.Array(docs.take(2))

      val next2 = evalPage(auth, first2 / "after")
      (next2 / "data") shouldEqual Value.Array(docs.take(4).drop(2))

      val first3 =
        evalOk(auth, s"User.all().concat(User.all().reverse()).paginate(2)")
      (first3 / "data") shouldEqual Value.Array(docs.take(2))

      val next3 = evalPage(auth, first3 / "after")
      (next3 / "data") shouldEqual Value.Array(docs.take(4).drop(2))
    }

    ".first() returns the first element" in {
      val res = evalOk(auth, "User.all().first()")
      res shouldEqual docs.head
    }

    ".take(n) returns a set of the first n elements" in {
      val limited = evalOk(auth, "User.all().take(10).paginate()")
      (limited / "data") shouldEqual Value.Array(docs.take(10))

      limited should matchPattern {
        case Value.Struct.Full(fields, _, _, _)
            if fields.contains("data") && !fields.contains("after") =>
          ()
      }

      val first = evalOk(auth, "User.all().take(20).paginate()")
      (first / "data") shouldEqual Value.Array(docs.take(16))

      first should matchPattern {
        case Value.Struct.Full(fields, _, _, _)
            if fields.contains("data") && fields.contains("after") =>
          ()
      }

      val cont = evalPage(auth, first / "after")
      (cont / "data") shouldEqual Value.Array(docs.drop(16).take(4))
    }

    ".take(0).toArray() works" in {
      val nothing = evalOk(auth, "User.all().take(0).toArray()")
      nothing shouldEqual Value.Array()
    }

    ".take() has bounds checks" in {
      evalErr(auth, "User.all().take(-1)") should matchPattern {
        case QueryRuntimeFailure.Simple(
              "invalid_bounds",
              "expected `limit` to be greater than 0, received -1",
              _,
              Seq()) =>
      }
    }

    ".drop(n) elides the first n elements of a set" in {
      // Smaller than the default page size.
      val small = evalOk(auth, "User.all().drop(10).take(10).paginate()")
      small shouldEqual Value.Struct("data" -> Value.Array(docs.drop(10).take(10)))

      // Skip several pages.
      val large = evalOk(auth, "User.all().drop(32).take(10).paginate()")
      large shouldEqual Value.Struct("data" -> Value.Array(docs.drop(32).take(10)))
    }

    ".drop(n) can drop exactly all elems in a set" in {
      val res = evalOk(auth, "[1].toSet().drop(1).toArray()")
      res shouldEqual Value.Array()
    }

    ".where(pred) returns a set filtered based on `pred`" in {
      val res = evalOk(auth, "User.all().where(.foo <= 5).paginate()")
      res shouldEqual Value.Struct("data" -> Value.Array(docs.take(5)))
    }

    ".where(pred) doesn't return too many elements" in {
      val res = evalOk(auth, "[1, 10, 2, 3].toSet().where(v => v < 10).paginate(2)")
      (res / "data") shouldEqual Value.Array(Value.Int(1), Value.Int(2))

      val next = evalPage(auth, res / "after")
      (next / "data") shouldEqual Value.Array(Value.Int(3))
      (next / "after") shouldEqual Value.Null(Span.Null)
    }

    ".where(pred) works when it over-fetches across sets" in {
      evalOk(auth, "Collection.create({ name: 'Nums' })")
      evalOk(auth, "[1, 2, 2, 2].forEach(n => Nums.create({ number: n }))")

      val res1 = evalOk(
        auth,
        """|let set1 = Nums.all().map(.number)
           |let set2 = [2].toSet()
           |set1.concat(set2)
           |  .where((v) => v != 1)
           |  .paginate(2)
           |""".stripMargin
      )

      val fields = res1.asInstanceOf[Value.Struct.Full]
      val after = fields.fields("after").asInstanceOf[Value.SetCursor]
      val cur = Value.SetCursor.toBase64(after, None)

      noException should be thrownBy {
        evalOk(auth, s"Set.paginate('$cur')")
      }
    }

    ".flatMap doesn't paginate with a negative page size" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      evalOk(
        auth,
        """|User.create({ a: 1 })
           |User.create({ a: 10 })
           |User.create({ a: 2 })
           |User.create({ a: 3 })""".stripMargin
      )
      // the `inner` set returns 3 elements on a page size of two
      //
      // then, the flatMap() will try to fetch a second page with a size of -1,
      // which will break the reverse()'d set.
      val res1 = evalOk(
        auth,
        """|let inner = User.all().map(.a).where(v => v < 10)
           |[10, 20].toSet().flatMap(num => inner.map(v => v + num)).paginate(2)""".stripMargin
      )
      (res1 / "data") shouldBe Value.Array(Value.Int(11), Value.Int(12))

      val res2 = evalPage(auth, res1 / "after")
      res2 / "data" shouldEqual Value.Array(Value.Int(13), Value.Int(21))

      val res3 = evalPage(auth, res2 / "after")
      res3 / "data" shouldEqual Value.Array(Value.Int(22), Value.Int(23))
      res3 / "after" shouldEqual Value.Null(Span.Null)
    }

    ".map(fn) returns a set with the values mapped via `fn`" in {
      val res = evalOk(auth, "User.all().map(.foo).take(5).toArray()")
      res shouldEqual (1 to 5).map(Value.Int(_)).to(Value.Array)
    }

    ".map(.update()) is disallowed" in {
      // newDB sets the auth's database to RootDatabase, which has no account
      // flags. This test relies in an account flag override in CassandraHelper.
      evalOk(RootAuth, "Database.create({ name: 'map_update_test' })")
      val db = getDB(RootAuth.scopeID, "map_update_test").value
      val auth0 = SystemAuth(db.scopeID, db, AdminPermissions)
      mkColl(auth0, "User")
      mkDocs(auth0, "User", 100)
      evalErr(auth0, "User.all().map(.update({})).paginate()") should matchPattern {
        case QueryRuntimeFailure.Simple(
              "invalid_effect",
              "`update` performs a write, which is not allowed in set functions.",
              _,
              _) =>
      }
    }

    ".map(fn) works with at()" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      // make two docs
      evalOk(auth, "User.create({ name: 'A' })")
      val res = evalRes(auth, "User.create({ name: 'Joe' }).id")
      val ts = res.ts
      val id = res.value.to[Value.ID].value

      // now that we update Joe to Bob, the old cursor should still give us two
      // docs containing Joe.
      evalOk(auth, s"User.byId('$id')!.update({ name: 'Bob' })")

      // this will have two entries, both of which map to Joe
      val set =
        evalOk(
          auth,
          s"at (Time('$ts')) User.all().map(v => User.byId('$id')!.name).paginate(1)")
      set / "data" shouldEqual Value.Array(Value.Str("Joe"))

      val next = evalPage(auth, set / "after")
      next / "data" shouldEqual Value.Array(Value.Str("Joe"))

      // this will have the same result (note the .paginate() is outside the at())
      val set2 =
        evalOk(
          auth,
          s"(at (Time('$ts')) User.all().map(v => User.byId('$id')!.name)).paginate(1)")
      set2 / "data" shouldEqual Value.Array(Value.Str("Joe"))

      val next2 = evalPage(auth, set2 / "after")
      next2 / "data" shouldEqual Value.Array(Value.Str("Joe"))

      // this will have the _new_ result (note the .flatMap() is outside the at())
      val set3 =
        evalOk(
          auth,
          s"(at (Time('$ts')) User.all()).map(v => User.byId('$id')!.name).paginate(1)")
      set3 / "data" shouldEqual Value.Array(Value.Str("Bob"))

      val next3 = evalPage(auth, set3 / "after")
      next3 / "data" shouldEqual Value.Array(Value.Str("Bob"))
    }

    ".flatMap(fn) works with at()" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      // make two docs
      evalOk(auth, "User.create({ name: 'A' })")
      val res = evalRes(auth, "User.create({ name: 'Joe' }).id")
      val ts = res.ts
      val id = res.value.to[Value.ID].value

      // now that we update Joe to Bob, the old cursor should still give us two
      // docs containing Joe.
      evalOk(auth, s"User.byId('$id')!.update({ name: 'Bob' })")

      // this will have two entries, both of which map to Joe
      val set =
        evalOk(
          auth,
          s"at (Time('$ts')) User.all().flatMap(v => [User.byId('$id')!.name].toSet()).paginate(1)")
      set / "data" shouldEqual Value.Array(Value.Str("Joe"))

      val next = evalPage(auth, set / "after")
      next / "data" shouldEqual Value.Array(Value.Str("Joe"))

      // this will have the same result (note the .paginate() is outside the at())
      val set2 =
        evalOk(
          auth,
          s"(at (Time('$ts')) User.all().flatMap(v => [User.byId('$id')!.name].toSet())).paginate(1)")
      set2 / "data" shouldEqual Value.Array(Value.Str("Joe"))

      val next2 = evalPage(auth, set2 / "after")
      next2 / "data" shouldEqual Value.Array(Value.Str("Joe"))

      // this will have the _new_ value of `id` (note the .flatMap() is outside the
      // at())
      val set3 =
        evalOk(
          auth,
          s"(at (Time('$ts')) User.all()).flatMap(v => [User.byId('$id')!.name].toSet()).paginate(1)")
      set3 / "data" shouldEqual Value.Array(Value.Str("Bob"))

      val next3 = evalPage(auth, set3 / "after")
      next3 / "data" shouldEqual Value.Array(Value.Str("Bob"))
    }

    ".where(fn) works with at()" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")
      // make two docs
      evalOk(auth, "User.create({ name: 'A' })")
      val res = evalRes(auth, "User.create({ name: 'Joe' }).id")
      val ts = res.ts
      val id = res.value.to[Value.ID].value

      // now that we update Joe to Bob, the old cursor should still give us two
      // docs containing Joe.
      evalOk(auth, s"User.byId('$id')!.update({ name: 'Bob' })")

      // the where predicate should return true here, because the lambda is
      // evaluated before we updated the name to Bob.
      val set =
        evalOk(
          auth,
          s"""|at (Time('$ts')) {
              |  User
              |    .all()
              |    .where(v => User.byId('$id')!.name == 'Joe')
              |    .map(.name)
              |    .paginate(1)
              |}""".stripMargin
        )
      set / "data" shouldEqual Value.Array(Value.Str("A"))

      val next = evalPage(auth, set / "after")
      next / "data" shouldEqual Value.Array(Value.Str("Joe"))
    }

    "misc at() test" in {
      val auth = newDB
      evalOk(auth, "Collection.create({ name: 'User' })")

      // there is one doc, which will be updated from alice to bob to carol.

      val res1 = evalRes(auth, "User.create({ name: 'Alice' }).id")
      val ts1 = res1.ts
      val id = res1.value.to[Value.ID].value

      val res2 = evalRes(auth, s"User.byId('$id')!.update({ name: 'Bob' })")
      val ts2 = res2.ts
      val res3 = evalRes(auth, s"User.byId('$id')!.update({ name: 'Carol' })")
      val ts3 = res3.ts

      evalOk(auth, s"User.byId('$id')!.update({ name: 'Bob' })")
      val set = evalOk(
        auth,
        s"""|let ts1 = Time.fromString('$ts1')
            |let ts2 = Time.fromString('$ts2')
            |let ts3 = Time.fromString('$ts3')
            |
            |// this fetches two duplicate users at the time of Alice
            |let set = at (ts1) {
            |  User.all().concat(User.all())
            |}
            |
            |// this fetches 'id' at the time of Bob
            |let set2 = at (ts2) {
            |  set.map(v => [v, User.byId('$id')!])
            |}
            |
            |// this fetches 'id' at the time of Carol
            |let set3 = at (ts3) {
            |  set2.map(arr => arr.append(User.byId('$id')!))
            |}
            |
            |// now do our reads
            |set3.map(.map(.name)).paginate(1)
            |""".stripMargin
      )

      set / "data" shouldEqual Value.Array(
        Value.Array(Value.Str("Alice"), Value.Str("Bob"), Value.Str("Carol")))

      // because the initial set was User.all() twice, we should have a second
      // element. this is here to test reification.
      val next = evalPage(auth, set / "after")
      next / "data" shouldEqual Value.Array(
        Value.Array(Value.Str("Alice"), Value.Str("Bob"), Value.Str("Carol")))
    }

    ".reverse() returns a reversed index set" in {
      val res = evalOk(auth, "User.all().reverse().paginate()")
      (res / "data") shouldEqual Value.Array(docs.reverse.take(16))

      val next = evalPage(auth, res / "after")
      (next / "data") shouldEqual Value.Array(docs.reverse.take(32).drop(16))
    }

    ".reverse() composes with .take(n)" in {
      val first = evalOk(auth, "User.all().take(10).reverse().paginate()")
      (first / "data") shouldEqual Value.Array(docs.take(10).reverse)

      val second = evalOk(auth, "User.all().reverse().take(10).paginate()")
      (second / "data") shouldEqual Value.Array(docs.reverse.take(10))
    }

    ".reverse() composes with .concat()" in {
      val first =
        evalOk(auth, "User.all().concat([1, 2, 3].toSet()).reverse().paginate()")
      (first / "data") shouldEqual Value.Array(
        ArraySeq(Value.Int(3), Value.Int(2), Value.Int(1)) ++
          docs.reverse.take(13))

      val second =
        evalOk(auth, "User.all().reverse().concat([1, 2, 3].toSet()).paginate(200)")
      (second / "data") shouldEqual Value.Array(
        docs.reverse ++
          ArraySeq(Value.Int(1), Value.Int(2), Value.Int(3)))
    }

    ".reverse() composes with where(pred)" in {
      val first = evalOk(auth, "User.all().reverse().where(.foo <= 5).paginate()")
      (first / "data") shouldEqual Value.Array(docs.take(5).reverse)

      val second = evalOk(auth, "User.all().where(.foo <= 5).reverse().paginate()")
      (second / "data") shouldEqual Value.Array(docs.take(5).reverse)
    }

    ".reverse() composes with flatMap()" in {
      val forward = (100 to 1 by -1) flatMap { i =>
        val flip = if (i % 2 == 0) {
          "heads"
        } else {
          "tails"
        }

        Seq(Value.Int(i), Value.Str(flip))
      }

      val reversed = (100 to 1 by -1) flatMap { i =>
        val flip = if (i % 2 == 0) {
          "heads"
        } else {
          "tails"
        }

        Seq(Value.Str(flip), Value.Int(i))
      }

      // User.all iterates in reverse, but [ foo, flip ] stay in forward order.
      val first = evalOk(
        auth,
        "User.all().reverse().flatMap(doc => [doc.foo, doc.flip].toSet()).paginate()")
      (first / "data") shouldEqual forward.take(16).to(Value.Array)

      // User.all iterates in reverse, but order changes to [ flip,
      // foo ], as though flattening _then_ reversing.
      val second = evalOk(
        auth,
        "User.all().flatMap(doc => [doc.foo, doc.flip].toSet()).reverse().paginate()")
      (second / "data") shouldEqual reversed.take(16).to(Value.Array)
    }

    ".reverse() composes with map()" in {
      val first = evalOk(auth, "User.all().reverse().map(.foo).paginate()")
      (first / "data") shouldEqual (100 to 85 by -1)
        .map(Value.Int(_))
        .to(Value.Array)

      val second = evalOk(auth, "User.all().map(.foo).reverse().paginate()")
      (second / "data") shouldEqual (100 to 85 by -1)
        .map(Value.Int(_))
        .to(Value.Array)
    }

    ".reverse() composes with order(ord)" in {
      val first = evalOk(auth, "User.all().reverse().order(.foo).paginate()")
      (first / "data") shouldEqual Value.Array(docs.take(16))

      val second = evalOk(auth, "User.all().order(.foo).reverse().paginate()")
      (second / "data") shouldEqual Value.Array(docs.reverse.take(16))
    }

    ".order(ord) returns a set with the values ordered based on `ord`" in {
      val res1 = evalOk(auth, "User.all().order(.foo).take(5).toArray()")
      res1 shouldEqual Value.Array(docs.take(5))

      val res2 = evalOk(auth, "User.all().order(asc(.foo)).take(5).toArray()")
      res2 shouldEqual Value.Array(docs.take(5))

      val res3 = evalOk(auth, "User.all().order(desc(.foo)).take(5).toArray()")
      res3 shouldEqual Value.Array(docs.reverse.take(5))
    }

    ".order() supports ordering on multiple fields" in {
      val res1 = evalOk(auth, "User.all().order(.flip, .foo).take(5).toArray()")
      res1 shouldEqual Value.Array(
        docs.zipWithIndex.filter(_._2 % 2 == 1).map(_._1).take(5))
    }

    ".order() can order by id" in {
      val res1 = evalOk(auth, "User.all().order(.id).take(5).toArray()")
      res1 shouldEqual Value.Array(docs.take(5))

      val res2 = evalOk(auth, "User.all().order(desc(.id)).take(5).toArray()")
      res2 shouldEqual Value.Array(docs.reverse.take(5))
    }

    ".firstWhere(pred) returns the first element which matches `pred`" in {
      val res = evalOk(auth, "User.all().firstWhere(.foo <= 5)")
      res shouldEqual docs.head
    }

    ".forEach(func) executes `func` on each element in the set" in {
      val res =
        evalOk(auth, "User.all().forEach(d => d.update({ foo: d.foo * -1 }))")
      res shouldEqual Value.Null(Span.Null)

      val res2 = evalOk(auth, "User.all().map(.foo).take(100).toArray()")
      res2 shouldEqual (1 to 100).map(i => Value.Int(i * -1)).to(Value.Array)

      evalOk(auth, "User.all().forEach(d => d.update({ foo: d.foo * -1 }))")

      val res3 = evalOk(auth, "User.all().map(.foo).take(100).toArray()")
      res3 shouldEqual (1 to 100).map(Value.Int(_)).to(Value.Array)
    }

    ".fold(seed, func) reduces the set starting with `seed`, using `func`" in {
      val res = evalOk(auth, "User.all().fold(0, (m, d) => m + d.foo)")
      res shouldEqual Value.Int((1 to 100).sum)
    }

    ".count() returns the # of elements in the set" in {
      val res = evalOk(auth, "User.all().count()")
      res shouldEqual Value.Int(100)
    }

    ".distinct() returns unique elements" in {
      val res =
        evalOk(auth, "User.all().order(.flip).map(.flip).distinct().paginate()")
      res shouldEqual Value.Struct(
        "data" -> Value.Array(Value.Str("heads"), Value.Str("tails")))

      val heads =
        evalOk(auth, "User.all().order(.flip).map(.flip).distinct().paginate(1)")
      (heads / "data") shouldEqual Value.Array(Value.Str("heads"))

      val tails = evalPage(auth, heads / "after")
      tails shouldEqual Value.Struct("data" -> Value.Array(Value.Str("tails")))
    }

    ".distinct() orders by doc ID" in {
      evalOk(
        auth,
        """|User.definition.update({
           |  indexes: {
           |    withFlip: {
           |      terms: [],
           |      values: [{ field: "flip" }]
           |    }
           |  }
           |})""".stripMargin
      )

      evalOk(
        auth,
        "User.definition.indexes.withFlip?.status"
      ).as[String] shouldBe CollectionIndex.Status.Complete.asStr

      val nat = evalOk(auth, "User.withFlip().distinct().paginate(100)")

      // A naturally-ordered set should distinct() by doc ID, and
      // result in the same set.
      (nat / "data").asInstanceOf[Value.Array].elems should
        contain allElementsOf (docs)
    }

    ".any() finds the first match" in {
      val heads = evalOk(auth, """User.all().any(.flip == "heads")""")
      heads shouldEqual Value.True

      val none = evalOk(auth, """User.all().any(.foo == 101)""")
      none shouldEqual Value.False
    }

    ".every() finds the first non-match" in {
      val heads = evalOk(auth, """User.all().every(.flip == "heads")""")
      heads shouldEqual Value.False

      val all = evalOk(auth, """User.all().every(.foo < 101)""")
      all shouldEqual Value.True
    }

    ".flatMap paginates across a small inner set" in {
      val expected = (1 to 100) flatMap { i =>
        val flip = if (i % 2 == 0) {
          "heads"
        } else {
          "tails"
        }

        Seq(Value.Int(i), Value.Str(flip))
      }

      val first =
        evalOk(
          auth,
          """User.all().flatMap(d => [d.foo, d.flip].toSet()).paginate()""")
      (first / "data") shouldEqual expected.take(16).to(Value.Array)

      val next = evalPage(auth, first / "after")
      (next / "data") shouldEqual expected.drop(16).take(16).to(Value.Array)
    }

    ".flatMap() paginates its inner set" in {
      val elems = docs.zipWithIndex
      val expected = elems flatMap { case (_, i) =>
        val even = i % 2
        elems collect { case (d, j) if j % 2 == even => d }
      }

      val first =
        evalOk(
          auth,
          """User.all().flatMap(d => User.all().where(.flip == d.flip)).paginate()""")
      (first / "data") shouldEqual expected.take(16).to(Value.Array)

      val next = evalPage(auth, first / "after")
      (next / "data") shouldEqual expected.drop(16).take(16).to(Value.Array)
    }

    ".flatMap() twice works" in {
      // this specific case was found in fuzzing, and only manifests when:
      // - the base set is an index set
      // - flatMap is called twice
      // - at least one of the sets returned from the flatMap lambda has at least
      //   2 elements
      val first =
        evalOk(
          auth,
          "User.all().flatMap(d => [1, 2].toSet()).flatMap(v => [].toSet()).paginate(1)")
      (first / "data") shouldEqual Value.Array()
      (first / "after") shouldEqual Value.Null(Span.Null)
    }

    "limits" - {
      "fail when materializing too many elements at once" in {
        val intp = new FQLInterpreter(auth)
        val set = evalOk(auth, "User.all()").to[ValueSet]
        (ctx ! set.materialize(intp, limit = 10)) should
          matchPattern {
            case Result.Err(QueryRuntimeFailure.Simple(
                  "value_too_large",
                  "Value too large: exceeded maximum number of elements (limit=10).",
                  _,
                  _)) =>
          }
      }
    }

    "Set.sequence" in {
      for {
        start <- 0 until 10
        stop  <- start until 10
        size  <- 1 until 10
        range = (start until stop) map { Value.Int(_) }
      } yield {
        def check(set: String)(expect: Iterable[Value]) = {
          val query = s"$set.paginate($size)"
          withClue(query) {
            evalAllPages(auth, query) should
              contain theSameElementsInOrderAs expect
          }
        }

        check(s"Set.sequence($start, $stop)")(range)
        check(s"Set.sequence($start, $stop).reverse()")(range.reverse)
        check(s"Set.sequence($start, $stop).where(n => n % 2 == 0)") {
          range filter { _.value % 2 == 0 }
        }
        check(s"Set.sequence($start, $stop).reverse().where(n => n % 2 == 0)") {
          range.reverse filter { _.value % 2 == 0 }
        }
      }
    }

    "Set.sequence gives zero elements" in {
      def checkEmpty(query: String)(implicit pos: Position) = {
        val page = evalOk(auth, query)
        page / "data" shouldBe Value.Array()
        page / "after" shouldBe Value.Null(Span.Null)
      }

      checkEmpty("Set.sequence(0, 0).paginate(2)")
      checkEmpty("Set.sequence(1, 0).paginate(2)")
      checkEmpty("Set.sequence(2, 0).paginate(2)")

      checkEmpty("Set.sequence(0, 0).reverse().paginate(2)")
      checkEmpty("Set.sequence(1, 0).reverse().paginate(2)")
      checkEmpty("Set.sequence(2, 0).reverse().paginate(2)")
    }

    "Set.sequence can aggregate" in {
      evalOk(auth, "Set.sequence(1, 4).aggregate(0, (a, b) => a + b)") shouldBe
        Value.Int(6)
    }

    "ranges" - {
      def withRangeIndexes(test: => Any) = {
        evalOk(
          auth,
          """|User.definition.update({
             |  indexes: {
             |    fooRange: {
             |      values: [ { field: "foo" } ]
             |    },
             |    flipRange: {
             |      values: [ { field: "flip" } ]
             |    },
             |    flipAndFooRange: {
             |      values: [
             |        { field: "flip", order: "desc" },
             |        { field: "foo" }
             |      ]
             |    }
             |  }
             |})""".stripMargin
        )
        test
      }

      "can start form a position" in withRangeIndexes {
        val elems =
          evalOk(auth, "User.fooRange({ from: 50 }).map(.foo).toArray()")
            .to[Value.Array]
            .elems

        elems should have size 51

        all(elems) should matchPattern {
          case Value.Int(n) if n >= 50 =>
        }
      }

      "can stop at a position" in withRangeIndexes {
        val elems =
          evalOk(auth, "User.fooRange({ to: 50 }).map(.foo).toArray()")
            .to[Value.Array]
            .elems

        elems should have size 50

        all(elems) should matchPattern {
          case Value.Int(n) if n <= 50 =>
        }
      }

      "can define a range inteval" in withRangeIndexes {
        val elems =
          evalOk(auth, "User.fooRange({ from: 50, to: 60 }).map(.foo).toArray()")
            .to[Value.Array]
            .elems

        elems should have size 11

        all(elems) should matchPattern {
          case Value.Int(n) if n >= 50 && n <= 60 =>
        }
      }

      "can range with a prefix" in withRangeIndexes {
        val elems =
          evalOk(
            auth,
            "User.flipAndFooRange({ from: 'heads' }).map(.flip).toArray()")
            .to[Value.Array]
            .elems

        elems should have size 50
        all(elems) should matchPattern { case Value.Str("heads") => }
      }

      "can range with a prefix interval" in withRangeIndexes {
        val res =
          evalOk(
            auth,
            """|let set = User.flipAndFooRange({
               |  from: 'heads',
               |  to: ['heads', 30]
               |})
               |let res = set { flip, foo }
               |res.toArray()
               |""".stripMargin
          )

        val elems = res.to[Value.Array].elems
        elems should have size 15

        all(elems) should matchPattern {
          case Value.Struct.Full(fields, _, _, _)
              if fields("flip") == Value.Str("heads") &&
                fields("foo").as[Int] <= 30 =>
        }
      }

      "can range with document id" in withRangeIndexes {
        val first = docs(0).id.subID.toLong
        val third = docs(2).id.subID.toLong

        val res1 =
          evalOk(
            auth,
            s"""|let set = User.all({
                |  from: User.byId('$first'),
                |  to: User.byId('$third')
                |})
                |let res = set { flip, id }
                |res.toArray()
                |""".stripMargin
          )

        val res2 =
          evalOk(
            auth,
            s"""|let set = User.all({
                |  from: User.byId('$first').id,
                |  to: User.byId('$third').id
                |})
                |let res = set { flip, id }
                |res.toArray()
                |""".stripMargin
          )

        val res3 =
          evalOk(
            auth,
            s"""|let set = User.all({
                |  from: '$first',
                |  to: '$third'
                |})
                |let res = set { flip, id }
                |res.toArray()
                |""".stripMargin
          )

        val res4 =
          evalOk(
            auth,
            s"""|let set = User.all({
                |  from: $first,
                |  to: $third
                |})
                |let res = set { flip, id }
                |res.toArray()
                |""".stripMargin
          )

        res1 shouldEqual res2
        res1 shouldEqual res3
        res1 shouldEqual res4

        val elems = res1.to[Value.Array].elems
        elems should have size 3

        all(elems) should matchPattern {
          case Value.Struct.Full(fields, _, _, _)
              if fields("id").as[Long] >= first && fields("id").as[Long] <= third =>
        }
      }

      "can paginate a range" in withRangeIndexes {
        def paginate(set: String) = {
          val data = Seq.newBuilder[Value]
          var cursor = null: String
          var continue = true

          while (continue) {
            val query = cursor match {
              case null => set
              case _    => s"Set.paginate($cursor)"
            }

            val page = evalOk(auth, query).to[Value.Struct.Full]
            data ++= page.fields("data").as[Vector[Value]]
            continue = page.fields.contains("after")

            if (continue) {
              val next =
                Value.SetCursor.toBase64(
                  (page / "after").asInstanceOf[Value.SetCursor],
                  None)
              cursor = s"'${next.toString()}'"
            }
          }

          data.result()
        }

        paginate(
          "User.fooRange({ from: 10, to: 90 }).map(.foo).paginate()"
        ) shouldEqual (10 to 90).map { Value.Int(_) }

        paginate(
          "User.fooRange({ from: 10, to: 90 }).reverse().map(.foo).paginate()"
        ) shouldEqual (10 to 90).reverse.map { Value.Int(_) }
      }

      "out-of-order prefixes result in an empty set" in withRangeIndexes {
        evalOk(
          auth,
          "User.fooRange({ from: 90, to: 1 }).toArray()"
        ).to[Value.Array].elems shouldBe empty
      }

      "require left or right bound" in withRangeIndexes {
        evalErr(
          auth,
          "User.fooRange({})",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: should be an object contining `{ from }`, `{ to }`, or `{ from, to }` fields",
                _,
                _) =>
        }
      }

      "fail to parse invalid range argument" in withRangeIndexes {
        evalErr(
          auth,
          "User.fooRange(123)",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: should be an object contining `{ from }`, `{ to }`, or `{ from, to }` fields",
                _,
                _) =>
        }
      }

      "fail to parse non-persistable range bound" in withRangeIndexes {
        evalErr(
          auth,
          "User.fooRange({ from: Collection })",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: The `from` field should be either an indexable value or an array of values",
                _,
                _) =>
        }

        evalErr(
          auth,
          "User.fooRange({ from: 1234, to: Collection })",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: The `to` field should be either an indexable value or an array of values",
                _,
                _) =>
        }
      }

      "fail to parse long prefix" in withRangeIndexes {
        evalErr(
          auth,
          "User.flipAndFooRange({ from: [123, 456, 'not covered'] })",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: The field `from` can have at most 2 values",
                _,
                _) =>
        }
      }

      "fail to parse non-doc id for no-value index" in withRangeIndexes {
        evalErr(
          auth,
          "User.all({ from: 'foo' })",
          typecheck = false
        ) should matchPattern {
          case QueryRuntimeFailure.Simple(
                "invalid_argument",
                "invalid argument `range`: The `from` field must be a document id.",
                _,
                _) =>
        }
      }
    }
  }

  "partials" - {
    def setup: Auth = {
      val auth = newAuth
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'User',
           |  indexes: {
           |    names: {
           |      values: [{ field: 'name' }]
           |    }
           |  }
           |})
           |""".stripMargin
      )
      evalOk(auth, "User.create({ name: 'Joe' })")
      auth
    }

    ".first() returns a partial value" in {
      val auth = setup

      val res1 = evalOk(auth, "User.names().first()!.data")
      res1 shouldBe a[Value.Struct.Partial]

      val res2 = evalOk(auth, "User.names().first()!.name")
      res2 shouldEqual Value.Str("Joe")
    }

    ".last() returns a partial value" in {
      val auth = setup

      val res1 = evalOk(auth, "User.names().last()!.data")
      res1 shouldBe a[Value.Struct.Partial]

      val res2 = evalOk(auth, "User.names().last()!.name")
      res2 shouldEqual Value.Str("Joe")
    }

    ".toArray() returns partial values" in {
      val auth = setup

      val res1 = evalOk(auth, "User.names().toArray()[0].data")
      res1 shouldBe a[Value.Struct.Partial]

      val res2 = evalOk(auth, "User.names().toArray()[0].name")
      res2 shouldEqual Value.Str("Joe")
    }

    ".paginate() returns partial values" in {
      val auth = setup

      val res1 = evalOk(auth, "User.names().paginate(1).data[0].data")
      res1 shouldBe a[Value.Struct.Partial]

      val res2 = evalOk(auth, "User.names().paginate(1).data[0].name")
      res2 shouldEqual Value.Str("Joe")
    }

    ".aggregate() returns partial values" in {
      val auth = setup

      // you can't really use aggregate without map, so the map collections partials
      // here.
      val res1 = evalOk(
        auth,
        """|User.names()
           |  .map(v => [v])
           |  .aggregate([], (a, b) => a.concat(b))[0].data""".stripMargin)
      res1 shouldBe a[Value.Struct.Partial]

      val res2 = evalOk(
        auth,
        """|User.names()
           |  .map(v => [v])
           |  .aggregate([], (a, b) => a.concat(b))[0].name""".stripMargin)
      res2 shouldEqual Value.Str("Joe")
    }
  }

  "take and drop should work" in {
    val auth = newDB

    evalOk(auth, "Set.sequence(0, 32000).take(110).toArray()") shouldBe (
      Value.Array((0 until 110).map(Value.Int(_)).to(ArraySeq))
    )
    evalOk(auth, "Set.sequence(0, 32000).drop(100).take(10).toArray()") shouldBe (
      Value.Array((100 until 110).map(Value.Int(_)).to(ArraySeq))
    )
    evalOk(auth, "Set.sequence(0, 32000).take(110).drop(100).toArray()") shouldBe (
      Value.Array((100 until 110).map(Value.Int(_)).to(ArraySeq))
    )

    // Edge case: we need to fetch 3 pages:
    // 1. 0..16000
    // 2. 16000..32000
    // 3. 32000..33000
    //
    // Then, we need to splice the 2nd and 3rd pages together.
    val actual =
      evalOk(auth, "Set.sequence(0, 50000).drop(18000).take(16000).toArray()")
        .asInstanceOf[Value.Array]

    // Produces better test output.
    actual.elems(0) shouldBe Value.Int(18000)
    actual.elems(15999) shouldBe Value.Int(33999)

    actual.elems.map(_.asInstanceOf[Value.Int].value) shouldBe (18000 until 34000)
  }
}
