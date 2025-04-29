package fauna.model.test

import fauna.auth.Auth
import fauna.exec.{
  ImmediateExecutionContext,
  Observable,
  OverflowStrategy,
  Publisher
}
import fauna.lang.clocks.Clock
import fauna.lang.Timestamp
import fauna.model.runtime.fql2.serialization.MaterializedValue
import fauna.model.runtime.fql2.Result
import fauna.model.runtime.stream.{
  Event,
  EventFilter,
  EventTransformer,
  EventTranslator,
  EventType
}
import fauna.repo.schema.Path
import fauna.repo.service.stream.TxnResult
import fauna.repo.values.Value
import fql.ast.{ Expr, Span }
import scala.collection.immutable.ArraySeq

class EventTranslatorSpec extends FQL2Spec with StreamContextHelpers {

  implicit val ec = ImmediateExecutionContext

  var txns: Observable[TxnResult] = _
  var pub: Publisher[TxnResult] = _
  var writeTxnTimes: Seq[Timestamp] = _
  var auth: Auth = _

  before {
    val (pub, txns) = Observable.gathering[TxnResult](OverflowStrategy.unbounded)
    this.pub = pub
    this.txns = txns
    writeTxnTimes = Seq.empty
    auth = newDB
  }

  "EventTranslator" - {
    "set" - {
      runTestsWith {
        EventTranslator.forSets(auth, Vector.empty, Seq.empty, _, _)
      }
    }

    "doc" - {
      runTestsWith {
        EventTranslator.forDocs(auth, _, _)
      }
    }

    def runTestsWith(
      mkTranslator: (EventFilter, EventTransformer) => EventTranslator
    ) = {
      def noFilter = mkTranslator(EventFilter.empty, EventTransformer.empty)

      "start" in {
        val pubTime = Clock.time
        pub.publish(TxnResult(pubTime, dispatchTS = Clock.time, Vector.empty))
        pub.close()
        gatherEvents(noFilter) should contain only Event.Status(pubTime)
      }

      "add" in {
        evalOk(auth, "Collection.create({ name: 'Foo' })")
        evalAndPublishWrites("Foo.create({})")

        val events = gatherEvents(noFilter)
        events should have size 1
        inside(events.head) { case event: Event.Data =>
          event.eventType shouldBe EventType.Add
          event.cursor.ts shouldBe writeTxnTimes.head
        }
      }

      "update" in {
        evalOk(auth, "Collection.create({ name: 'Foo' })")
        evalOk(auth, "Foo.create({ name: 'Foo' })")
        evalAndPublishWrites("Foo.all().first()!.update({ name: 'Bar' })")

        val events = gatherEvents(noFilter)
        events should have size 1
        inside(events.head) { case event: Event.Data =>
          event.eventType shouldBe EventType.Update
          event.cursor.ts shouldBe writeTxnTimes.head
        }
      }

      "remove" in {
        evalOk(auth, "Collection.create({ name: 'Foo' })")
        evalOk(auth, "Foo.create({ name: 'Foo' })")
        evalAndPublishWrites("Foo.all().first()!.delete()")

        val events = gatherEvents(noFilter)
        events should have size 1
        inside(events.head) { case event: Event.Data =>
          event.eventType shouldBe EventType.Remove
          event.cursor.ts shouldBe writeTxnTimes.head
        }
      }

      "trackes watched fields" in {
        evalOk(auth, "Collection.create({ name: 'Foo' })")
        evalOk(auth, "Foo.create({ name: 'Foo' })")
        evalAndPublishWrites(
          "Foo.all().first()!.update({ name: 'Bar' })", // keep
          "Foo.all().first()!.update({ name: 'Bar' })", // discard
          "Foo.all().first()!.update({ name: 'Baz' })" // keep
        )
        val filter = new EventFilter(Seq(Path(Right("name"))))
        val transformer = mkTranslator(filter, EventTransformer.empty)
        gatherEvents(transformer) should have size 2
      }
    }

    "stops at first error" in {
      mkColl(auth, "StreamColl")

      // create a transform that will read non existent
      // var to create an error
      val readTransform = new EventTransformer.Project(
        Value.Lambda(
          params = ArraySeq(Some("arg")),
          vari = None,
          expr = Expr.Id("doc", Span.Null),
          closure = Map.empty
        )
      )

      // query to would create 2 event errors
      // if we don't stop at the first one
      val output = eval(
        auth,
        """
          |StreamColl.create({})
          |StreamColl.create({})
          |""".stripMargin)

      publishWrites(Seq(output))

      val transformer = new EventTransformer(Seq(readTransform))
      val events = gatherEvents(
        EventTranslator.forSets(
          auth,
          terms = Vector.empty,
          coveredValues = Seq.empty,
          transformer = transformer
        ))

      // ensure only one error is returned
      events.size shouldEqual 1

      inside(events.head) { case _: Event.Error => }
    }

    "snapshots reads at the event transaction time" in {
      mkColl(auth, "TestColl")
      mkColl(auth, "StreamColl")
      val doc =
        evalOk(auth, """TestColl.create({ name: "pre-update" })""")
          .to[Value.Doc]

      // create a transform that will read the above created doc
      // at event translation time
      val readTransform = new EventTransformer.Project(
        Value.Lambda(
          params = ArraySeq(Some("arg")),
          vari = None,
          expr = Expr.Id("doc", Span.Null),
          closure = Map("doc" -> doc)
        )
      )

      // query to create first event, event from this should read pre-update
      val output = eval(auth, "StreamColl.create({})")

      // update to post-update prior to event translation
      evalOk(auth, """TestColl.all().first()!.update({ name: "post-update" })""")

      // query to create second event, this event should read post-update
      val output2 = eval(auth, "StreamColl.create({})")

      publishWrites(Seq(output, output2))

      // trigger event translation after post-update update has occurred
      val transformer = new EventTransformer(Seq(readTransform))
      val events = gatherEvents(
        EventTranslator.forSets(
          auth,
          terms = Vector.empty,
          coveredValues = Seq.empty,
          transformer = transformer
        ))
      events.size shouldEqual 2

      inside(events.head) { case event: Event.Data =>
        event.data.docs(MaterializedValue.DocKey(doc)) match {
          case Value.Struct.Full(fields, _, _, _) =>
            fields("name") shouldEqual Value.Str("pre-update")
          case v =>
            fail(s"expected materialized doc value to Value.Struct.Full, got $v")
        }
      }

      inside(events(1)) { case event: Event.Data =>
        event.data.docs(MaterializedValue.DocKey(doc)) match {
          case Value.Struct.Full(fields, _, _, _) =>
            fields("name") shouldEqual Value.Str("post-update")
          case v =>
            fail(s"expected materialized doc value to Value.Struct.Full, got $v")
        }
      }
    }
  }

  private def gatherEvents(translator: EventTranslator): Seq[Event] = {
    val events =
      txns flatMap {
        ctx ! translator.translate(_).map {
          Observable.from(_)
        }
      }
    await(events.sequenceF).flatMap(_.event)
  }

  private def evalAndPublishWrites(queries: String*): Unit =
    publishWrites(queries map { eval(auth, _) })

  private def publishWrites(output: Seq[Output]) = {
    output.foreach { out =>
      out.res match {
        case Result.Ok(res) => writeTxnTimes = writeTxnTimes :+ res.ts
        case Result.Err(e) =>
          val msg = e.errors.map(_.renderWithSource(Map.empty)).mkString("\n\n")
          fail(s"unexpected eval fail:\n$msg")
      }

      val state = out.state.value
      val writes = state.readsWrites.allWrites.toVector
      pub.publish(TxnResult(writeTxnTimes.last, dispatchTS = Clock.time, writes))
    }
    pub.close()
  }
}
