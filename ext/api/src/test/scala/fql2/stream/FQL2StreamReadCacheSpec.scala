package fauna.api.test

import fauna.codex.json.JSValue
import fauna.prop.Prop
import org.scalatest.matchers.{ MatchResult, Matcher }

class FQL2StreamReadCacheSpec extends FQL2StreamSpecHelpers {

  "Set.single(..).toStream()" - {
    once("do not charge read ops on projections without FKs") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        doc  <- aDocument(db, coll)
        stream = s"Set.single($coll.byId($doc)!).toStream()"
        eventsF = subscribeQuery(stream, db, expect = 3)
      } yield {
        queryOk(s"$coll.byId($doc)!.update({ foo: 'bar' })", db)
        queryOk(s"$coll.byId($doc)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 1) // history read
        all(events.tail) should charge(readOps = 0)
      }
    }

    once("charge reads on projections with FKs") {
      for {
        db    <- aDatabase
        coll0 <- aCollection(db)
        doc0  <- aDocument(db, coll0)
        coll1 <- aCollection(db)
        doc1  <- aDocument(db, coll1)
        stream = s"Set.single($coll1.byId($doc1)!).toStream() { fk { data } }"
        eventsF = subscribeQuery(stream, db, expect = 3)
      } yield {
        queryOk(s"$coll1.byId($doc1)!.update({ fk: $coll0.byId($doc0)! })", db)
        queryOk(s"$coll1.byId($doc1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 1) // history read
        all(events.tail) should charge(readOps = 1)
      }
    }
  }

  ".all().toStream()" - {
    once("do not charge read ops when projecting id only") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = s"$coll.all().toStream() { id }"
        eventsF = subscribeQuery(stream, db, expect = 4)
      } yield {
        queryOk(s"$coll.create({ id : 1, foo: 'foo' })", db)
        queryOk(s"$coll.byId(1)!.update({ foo: 'bar' })", db)
        queryOk(s"$coll.byId(1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 8) // history read
        all(events.tail) should charge(readOps = 0)
      }
    }

    once("charge read ops when projecting on other fields") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
        stream = s"$coll.all().toStream()"
        eventsF = subscribeQuery(stream, db, expect = 4)
      } yield {
        queryOk(s"$coll.create({ id : 1, foo: 'foo' })", db)
        queryOk(s"$coll.byId(1)!.update({ foo: 'bar' })", db)
        queryOk(s"$coll.byId(1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 8) // history read
        all(events.tail) should charge(readOps = 1)
      }
    }
  }

  "<user-index>().toStream()" - {
    once("do not charge read ops when projecting covered values") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("idx"),
            terms = Prop.const(Seq(".foo")),
            values = Prop.const(Seq(".bar"))
          ))
        stream = s"$coll.idx(42).toStream() { id, foo, bar }"
        eventsF = subscribeQuery(stream, db, expect = 4)
      } yield {
        queryOk(s"$coll.create({ id : 1, foo: 42, bar: 'bar' })", db)
        queryOk(s"$coll.byId(1)!.update({ bar: 'baz' })", db)
        queryOk(s"$coll.byId(1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 1) // history read
        all(events.tail) should charge(readOps = 0)
      }
    }

    once("charge read ops when projecting other fields") {
      for {
        db <- aDatabase
        coll <- aCollection(
          db,
          anIndex(
            name = Prop.const("idx"),
            terms = Prop.const(Seq(".foo")),
            values = Prop.const(Seq(".bar"))
          ))
        stream = s"$coll.idx(42).toStream()"
        eventsF = subscribeQuery(stream, db, expect = 4)
      } yield {
        queryOk(s"$coll.create({ id : 1, foo: 42, bar: 'bar' })", db)
        queryOk(s"$coll.byId(1)!.update({ bar: 'baz' })", db)
        queryOk(s"$coll.byId(1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 1) // history read
        all(events.tail) should charge(readOps = 1)
      }
    }

    once("charge read ops when projecting on MVA indexes") {
      for {
        db   <- aDatabase
        coll <- aUniqueIdentifier
        _ = queryOk(
          s"""|Collection.create({
              |  name: '$coll',
              |  indexes: {
              |    idx: {
              |      values: [{ field: '.bar', mva: true }]
              |    }
              |  }
              |})
              |""".stripMargin,
          db
        )
        stream = s"$coll.idx().toStream() { bar }"
        eventsF = subscribeQuery(stream, db, expect = 4)
      } yield {
        queryOk(s"$coll.create({ id : 1, bar: [1] })", db)
        queryOk(s"$coll.byId(1)!.update({ bar: [2, 3] })", db)
        queryOk(s"$coll.byId(1)!.delete()", db)

        val events = await(eventsF)
        events(0) should charge(readOps = 8) // history read
        all(events.tail) should charge(readOps = 1)
      }
    }
  }

  private def charge(readOps: Int) =
    new Matcher[JSValue] {
      def apply(value: JSValue) = {
        val reads = (value / "stats" / "read_ops").as[Long]
        MatchResult(
          reads == readOps,
          s"charged $reads instead of $readOps read ops",
          s"charged $readOps read ops"
        )
      }
    }
}
