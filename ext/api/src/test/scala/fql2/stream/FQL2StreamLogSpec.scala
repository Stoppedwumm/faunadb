package fauna.api.test

import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.exec.ImmediateExecutionContext
import fauna.net.http.HTTPHeaders
import fauna.prop.api.CoreLauncher
import java.io.{ BufferedReader, FileReader }
import org.scalactic.source.Position
import scala.util.Using

class FQL2StreamLogSpec extends FQL2StreamSpecHelpers {

  /** Used to track the stream logs as the tests progress
    * so that for each test we are only looking at the
    * relevant stream.log entries.
    */
  private var logEventsConsumed = 0;

  "live" - {
    runTestsWith(isReplay = false)

    once("accrues metrics when an event errors") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } yield {
        val eventsF = getEvents(
          eventQuery = s"$coll.create({})".stripMargin,
          streamQuery = s"$coll.all().map(.first.last).toStream()",
          db,
          isReplay = false,
          expect = 2
        )

        val events = await(eventsF)
        (events(1) / "type").as[String] shouldBe "error"

        val logEvents = getLogEvents(expected = 1)

        logEvents.size shouldBe 1
        val logEvent = logEvents.head

        // start + error event
        logEvent.streamEvents shouldBe 2
        // 1 extra compute op for keeping the stream open
        logEvent.queryComputeOps shouldBe 3
        // replay read across 8 partitions + 1 doc reads for the creates
        logEvent.queryByteReadOps shouldBe 9
        logEvent.queryByteReadOps shouldEqual events.foldLeft(0L) { (sum, ev) =>
          sum + (ev / "stats" / "read_ops").as[Long]
        }
        logEvent.queryComputeOps shouldEqual (events.foldLeft(0L) { (sum, ev) =>
          sum + (ev / "stats" / "compute_ops").as[Long]
        }) + 1
      }
    }

    once(
      "returns a stream id in the response that matches the log entries, and includes global id in log entries") {
      for {
        db <- aDatabase
        childDb = queryOk(
          """
            |let db = Database.create({ name: "test_db" })
            |let secret = Key.create({ role: "server", database: "test_db" }).secret
            |{ global_id: db.global_id, secret: secret }
            |""".stripMargin,
          db
        )
        childDbGlobalID = (childDb / "global_id").as[String]
        childDbSecret = (childDb / "secret").as[String]
        coll = queryOk(
          """
            |let name = "TestColl"
            |Collection.create({ name: name })
            |name
            |""".stripMargin,
          childDbSecret
        ).as[String]
      } yield {
        val stream =
          queryOk(s"$coll.all().toStream()", childDbSecret).as[String]
        val req = streamRequest(stream)
        val response = client.api.post("/stream/1", req, childDbSecret).res
        response.code shouldBe OK
        val responseStreamID = response.header(HTTPHeaders.FaunaStreamID).toLong

        queryOk(
          s"""
             |$coll.create({})
             |$coll.create({})
             |""".stripMargin,
          childDbSecret)

        implicit val ec = ImmediateExecutionContext
        await(response.body.events.takeF(3))

        val logEvents = getLogEvents(1)
        logEvents.size shouldBe 1
        logEvents.foreach { le =>
          le.streamID shouldEqual responseStreamID
          le.globalIDPath shouldEqual Seq(db.globalID, childDbGlobalID)
        }
      }
    }
  }

  "replay" - {
    runTestsWith(isReplay = true)
  }

  def runTestsWith(isReplay: Boolean): Unit = {
    once("stream logs events in stream.log") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } yield {
        val eventsF = getEvents(
          eventQuery = s"""
               |$coll.create({})
               |$coll.create({})
               |$coll.create({})
               |""".stripMargin,
          streamQuery = s"$coll.all().toStream()",
          db,
          isReplay,
          expect = 4
        )

        val events = await(eventsF)

        val logEvents = getLogEvents(expected = 1)

        logEvents.size shouldBe 1
        val logEvent = logEvents.head

        // start + 3 creates
        logEvent.streamEvents shouldBe 4
        // 1 extra compute op for keeping the stream open
        logEvent.queryComputeOps shouldBe 5
        // replay read across 8 partitions + 3 doc reads for the creates
        logEvent.queryByteReadOps shouldBe 11
        logEvent.queryByteReadOps shouldEqual events.foldLeft(0L) { (sum, ev) =>
          sum + (ev / "stats" / "read_ops").as[Long]
        }
        logEvent.queryComputeOps shouldEqual (events.foldLeft(0L) { (sum, ev) =>
          sum + (ev / "stats" / "compute_ops").as[Long]
        }) + 1
      }
    }

    once("accrues ops for filtered events") {
      for {
        db   <- aDatabase
        coll <- aCollection(db)
      } yield {
        val eventsF = getEvents(
          // add one non filtered event so we can ensure all filtered events
          // have occurred prior to closing the stream.
          eventQuery = s"""
                          |$coll.create({ price: 40 })
                          |$coll.create({ price: 40 })
                          |$coll.create({ price: 40 })
                          |$coll.create({ price: 60 })
                          |""".stripMargin,
          streamQuery = s"$coll.all().where(.price > 50).toStream()",
          db,
          isReplay,
          expect = 2
        )

        await(eventsF)

        val logEvents = getLogEvents(expected = 1)

        logEvents.size shouldBe 1
        val logEvent = logEvents.head

        // start + 1 non filtered create
        logEvent.streamEvents shouldBe 2
        // 3 filtered creates
        logEvent.filteredEvents shouldBe 3
        // 1 extra compute op for keeping the stream open
        logEvent.queryComputeOps shouldBe 6
        // replay read across 8 partitions + 3 doc reads for the filtered creates +
        // 1 doc read for non filtered create
        logEvent.queryByteReadOps shouldBe 12
      }
    }
  }

  case class StreamLogEvent(
    streamID: Long,
    streamEvents: Long,
    filteredEvents: Long,
    queryByteReadOps: Long,
    queryComputeOps: Long,
    storageBytesRead: Long,
    globalIDPath: Seq[String]
  )

  private def getLogEvents(expected: Int)(
    implicit pos: Position): Seq[StreamLogEvent] = {

    def lineIterator(reader: BufferedReader): Iterator[String] = {
      new Iterator[String] {
        def hasNext: Boolean = reader.ready()

        def next(): String = reader.readLine()
      }
    }

    val streamLogFile =
      CoreLauncher
        .instanceDir(clusterInstanceID)
        .resolve("log")
        .resolve("stream.log")

    eventually {
      val events = Seq.newBuilder[StreamLogEvent]
      Using(new BufferedReader(new FileReader(streamLogFile.toFile))) { reader =>
        val iter = lineIterator(reader)
        while (iter.hasNext) {
          val line = iter.next()
          val jsonLine = JSON.parse[JSValue](line.getBytes)
          (jsonLine / "api_version").as[String] shouldEqual "10"
          val stats = (jsonLine / "stats")
          val streamLogEvent = StreamLogEvent(
            streamID = (jsonLine / "stream_id").as[Long],
            streamEvents = (stats / "stream_events").as[Long],
            filteredEvents = (stats / "filtered_events").as[Long],
            queryByteReadOps = (stats / "query_byte_read_ops").as[Long],
            queryComputeOps = (stats / "query_compute_ops").as[Long],
            storageBytesRead = (stats / "storage_bytes_read").as[Long],
            globalIDPath = (jsonLine / "global_id_path").as[Seq[String]]
          )
          events.addOne(streamLogEvent)
        }
      }.get
      val logEvents = events.result()
      val eventsToReturn = logEvents.drop(logEventsConsumed)
      eventsToReturn.size shouldEqual expected
      logEventsConsumed = logEvents.length
      eventsToReturn
    }
  }
}
