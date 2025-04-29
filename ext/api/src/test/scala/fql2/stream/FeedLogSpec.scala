package fauna.api.test

import fauna.codex.json.{ JSArray, JSValue }
import fauna.codex.json2.JSON
import fauna.prop.api.CoreLauncher
import java.io.{ BufferedReader, FileReader }
import scala.util.Using

class FeedLogSpec extends FeedSpecHelpers {
  once("adds read ops to the query log") {
    for {
      db   <- aDatabase
      coll <- aCollection(db)
      stream = queryOk(s"$coll.all().eventSource()", db).as[String]
      _ <- aDocument(db, coll) * 16
    } {
      val page = poll(stream, db)
      (page / "events").as[Seq[JSValue]] should have size 16
      (page / "has_next").as[Boolean] shouldBe false
      eventually {
        val logs = getFeedLogsForGlobalID(db.globalID)
        logs should have size 1

        // partitioned index read + 16 doc reads
        (logs.head / "stats" / "query_byte_read_ops").as[Long] shouldEqual 24L
        (page / "stats" / "read_ops").as[Long] shouldEqual 24L
      }
    }
  }

  private def getFeedLogsForGlobalID(globalID: String) = {
    val queryLogFile =
      CoreLauncher
        .instanceDir(clusterInstanceID)
        .resolve("log")
        .resolve("query.log")

    val feedLogEntries = Seq.newBuilder[JSValue]

    Using(new BufferedReader(new FileReader(queryLogFile.toFile))) { reader =>
      while (reader.ready()) {
        val line = reader.readLine()
        val jsonLine = JSON.parse[JSValue](line.getBytes)
        val logGlobalID = (jsonLine / "global_id_path")
          .asOpt[JSArray]
          .flatMap(_.value.headOption)
          .map(_.as[String])
        if (
          (jsonLine / "request" / "path")
            .as[String]
            .startsWith("/feed") && logGlobalID.contains(globalID)
        ) {
          feedLogEntries.addOne(jsonLine)
        }
      }
    }
    feedLogEntries.result()
  }

}
