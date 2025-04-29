package fauna.api.test

import fauna.api.test.FQL2APISpec
import fauna.codex.json.{ JSObject, JSValue }
import fauna.codex.json2.JSON
import fauna.exec.ImmediateExecutionContext
import fauna.prop.api.Database
import org.scalactic.source.Position
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

trait EventAssertions { _: FQL2APISpec =>

  protected def assertStatusEvent(event: JSValue)(implicit pos: Position) = {
    (event / "type").as[String] shouldBe "status"
    (event / "txn_ts").asOpt[Long] should not be (empty)
  }

  protected def assertDataEvent(event: JSValue, eventType: String)(
    implicit pos: Position) = {
    (event / "type").as[String] shouldBe eventType
    (event / "txn_ts").asOpt[Long] should not be (empty)
    (event / "cursor").asOpt[String] should not be (empty)
    val data = (event / "data")
    data.asOpt[JSValue] should not be (empty)
    data
  }

  protected def assertErrorEvent(event: JSValue, code: String)(
    implicit pos: Position) = {
    (event / "type").as[String] shouldBe "error"
    (event / "error" / "code").as[String] shouldBe code
    (event / "error" / "message").as[String] should not be empty
  }
}

class FQL2StreamSpecHelpers extends FQL2APISpec with EventAssertions {

  protected def getEvents(
    eventQuery: String,
    streamQuery: String,
    db: Database,
    isReplay: Boolean,
    expect: Int): Future[Seq[JSValue]] = {
    val stream = queryOk(streamQuery, db).as[String]
    if (isReplay) {
      queryOk(eventQuery, db)
      subscribeStream(stream, db, expect)
    } else {
      val evs = subscribeStream(stream, db, expect)
      queryOk(eventQuery, db)
      evs
    }
  }

  protected def subscribeQuery(query: String, db: Key, expect: Int)(
    implicit pos: Position): Future[Seq[JSValue]] = {
    subscribeStream(queryOk(query, db).as[String], db.toString, expect)
  }

  protected def subscribeStream(
    stream: String,
    key: Key,
    expect: Int,
    startTime: Option[Long] = None,
    cursor: Option[String] = None
  )(implicit pos: Position): Future[Seq[JSValue]] = {
    val req = streamRequest(stream, startTime, cursor)
    val response = client.api.post("/stream/1", req, key.toString).res
    response.code shouldBe OK

    implicit val ec = ImmediateExecutionContext

    /** We await the first event to ensure that our tests testing the live code path actually hit the live code path.
      * Otherwise it is possible for the writes in those tests to hit disk before we have done the event replay.
      */
    val eventReceived = Promise[Boolean]()
    val events = response.body.lines
      .map { ev =>
        val parsedEv = JSON.parse[JSValue](ev)
        if (!eventReceived.isCompleted) {
          eventReceived.success(true)
        }
        parsedEv
      }
      .takeF(expect)

    Await.result(eventReceived.future, 5.seconds)
    events
  }

  protected def streamRequest(
    token: String,
    startTime: Option[Long] = None,
    cursor: Option[String] = None
  ) = {
    val req = JSObject.newBuilder
    req += "token" -> token
    startTime foreach { st => req += "start_ts" -> st }
    cursor foreach { cur => req += "cursor" -> cur }
    req.result()
  }

  protected def await[A](f: Future[A]) = Await.result(f, 10.seconds)
}
