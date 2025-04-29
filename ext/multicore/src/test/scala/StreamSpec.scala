package fauna.multicore.test

import fauna.codex.json._
import fauna.codex.json2._
import fauna.exec._
import fauna.net.http._
import fauna.prop.api._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.util.Random

class StreamSpec extends Spec with DefaultQueryHelpers {

  implicit val ec = ImmediateExecutionContext

  val api1 = CoreLauncher.apiClient(1, mcAPIVers)
  val api2 = CoreLauncher.apiClient(2, mcAPIVers)

  def query(cli: HttpClient, body: HttpBody) = cli.query(body, "secret")
  def stream(cli: HttpClient, body: HttpBody) = cli.post("/stream", body, "secret")
  def await[A](f: Future[A]) = Await.result(f, 1.minute)

  override def beforeAll(): Unit = {
    CoreLauncher.launchMultiple(Seq(1, 2))
    init(CoreLauncher.adminClient(1, mcAPIVers), "dc1")
    join(CoreLauncher.adminClient(2, mcAPIVers), "dc1")
    waitForPing(Seq(api1, api2))

    query(
      api1,
      CreateCollection(
        MkObject(
          "name" -> "docs"
        ))) should respond(Created)

    query(
      api1,
      CreateIndex(
        MkObject(
          "name" -> "idx",
          "partitions" -> 8,
          "source" -> ClassRef("docs"),
          "terms" -> JSArray(MkObject("field" -> JSArray("data", "foo"))),
          "values" -> JSArray(MkObject("field" -> JSArray("data", "bar")))
        ))
    ) should respond(Created)
  }

  override def afterAll(): Unit =
    CoreLauncher.terminateAll()

  "StreamService" - {

    "works with remote subscriptions" in {
      val doc = query(api1, CreateF(ClassRef("docs"), MkObject()))
      doc should respond(Created)

      // Subscribe for the same ref in 2 different nodes.
      // One of the streams is guaranteed to be remote.
      val events1 = gatherEvents(stream(api1, doc.refObj), expect = 101)
      val events2 = gatherEvents(stream(api2, doc.refObj), expect = 101)

      for (i <- 1 to 100) {
        val cli = if (i % 2 == 0) api1 else api2
        val update = Update(doc.refObj, MkObject("data" -> MkObject("value" -> i)))
        query(cli, update) should respond(OK)
      }

      await(events1) should have size 101 // start event + 100 updates
      await(events1) should contain inOrderElementsOf await(events2)
    }

    "do not duplicate events on contention" in {
      val doc = query(api1, CreateF(ClassRef("docs"), MkObject()))
      doc should respond(Created)

      val events = gatherEvents(stream(api1, doc.refObj), expect = 201)

      def update(cli: HttpClient, expr: JSValue): Future[HttpResponse] =
        query(cli, expr) flatMap { res =>
          if (res.code == TransactionContention) {
            update(cli, expr)
          } else {
            Future.successful(res)
          }
        }

      val updates = for (i <- 1 to 200) yield {
        val cli = if (i % 2 == 0) api1 else api2
        val expr = Update(doc.refObj, MkObject("data" -> MkObject("value" -> i)))
        update(cli, expr)
      }

      all(updates) should respond(OK)
      await(events) should have size 201 // start event + 100 updates (no duplicates)
    }

    "works with partitioned streams" in {
      val events = 101
      val indexMatch = Match(IndexRef("idx"), "foo")
      val events1 = gatherEvents(stream(api1, indexMatch), expect = events)
      val events2 = gatherEvents(stream(api2, indexMatch), expect = events)
      // NB. The network bus splits messages larger that 4Kb. In this test, we want
      // to make sure that message split don't cause re-ordering issues.
      val largeField = Random.alphanumeric.take(5096).mkString

      val writes =
        for (i <- 1 until events) yield {
          val create =
            CreateF(
              ClassRef("docs"),
              MkObject(
                "data" -> MkObject(
                  "foo" -> "foo",
                  "bar" -> largeField
                )))

          val cli = if (i % 2 == 0) api1 else api2
          query(cli, create)
        }

      await(Future.sequence(writes))
      await(events1) should have size events
      await(events2) should contain inOrderElementsOf await(events2)
    }
  }

  private def gatherEvents(
    response: Future[HttpResponse],
    expect: Int): Future[Seq[JSValue]] = {

    val jsonValues =
      response.res.body.events map { buf =>
        JSON.parse[JSValue](buf)
      }
    jsonValues.takeF(expect)
  }
}
