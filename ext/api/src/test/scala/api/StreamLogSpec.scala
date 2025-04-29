package fauna.api.test

import fauna.codex.json.{ JSObject, JSValue }
import fauna.codex.json2.JSON
import fauna.exec.FaunaExecutionContext
import fauna.lang.syntax._
import fauna.prop.api.{ CoreLauncher, FaunaDB }
import java.io.{ BufferedReader, FileReader }
import org.scalactic.source.Position
import scala.util.Using

class StreamLogSpec extends API3Spec with StreamAPISupport {
  import FaunaExecutionContext.Implicits.global

  once("includes global id path in stream logs") {
    for {
      db <- aDatabase
      childDB = FaunaDB.makeDB(
        "child_db",
        client.api,
        apiVers,
        adminKey = db.adminKey)
      coll <- aCollection(childDB)
      doc  <- aDocument(coll, dataProp = jsObject(minSize = 1))
      objects <- (jsObject(minSize = 1) * 2) mapT { obj =>
        JSObject("data" -> obj)
      }
      events = subscribe(doc.refObj, childDB.key, expect = 2) {
        objects foreach { data =>
          api.query(Replace(doc.refObj, Quote(data)), childDB.key) should respond(OK)
        }
      }
    } {
      events.zip(objects) foreach { case (event, data) =>
        (event / "type").as[String] shouldBe "version"
        (event / "event" / "action").as[String] shouldBe "update"
        (event / "event" / "document") should matchJSON(doc.merge(data))
      }

      getLogEventsForGlobalIDPath(Seq(db.globalID, childDB.globalID), 2)
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

  private def getLogEventsForGlobalIDPath(globalIDPath: Seq[String], expected: Int)(
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
          (jsonLine / "api_version").as[String] shouldEqual "4"
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
          if (globalIDPath == streamLogEvent.globalIDPath) {
            events.addOne(streamLogEvent)
          }
        }
      }.get
      val logEvents = events.result()
      logEvents.size shouldEqual expected
      logEvents
    }
  }

}
