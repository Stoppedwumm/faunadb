package fauna.api.test

import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.prop.api.CoreLauncher
import java.io.{ BufferedReader, FileReader }
import org.scalactic.source.Position
import scala.concurrent.duration._
import scala.util.Using

class TaskLogSpec extends FQL2APISpec {
  test("emits global id path to the task log") {
    val db = aDatabase.sample
    val childDb = queryOk(
      """
        |let db = Database.create({ name: "test_db" })
        |let secret = Key.create({ role: "admin", database: "test_db" }).secret
        |{ global_id: db.global_id, secret: secret }
        |""".stripMargin,
      db
    )
    val childDBGlobalID = (childDb / "global_id").as[String]
    val childDBSecret = (childDb / "secret").as[String]
    updateSchemaOk(
      childDBSecret,
      "main.fsl" ->
        """|collection TestColl {
           |}
           |""".stripMargin
    )
    queryOk(
      """
        |Set.sequence(1, 1000).forEach(i => {
        |  TestColl.create({ name: "test", idx: i })
        |})
        |""".stripMargin,
      childDBSecret
    )
    updateSchemaOk(
      childDBSecret,
      "main.fsl" ->
        """|collection TestColl {
           |  index testIndex {
           |    terms [.idx]
           |  }
           |}
           |""".stripMargin
    )
    validateTaskLogGlobalIDPath(Seq(db.globalID, childDBGlobalID), 1)
  }

  private def validateTaskLogGlobalIDPath(globalIDPath: Seq[String], expected: Int)(
    implicit pos: Position) = {

    def lineIterator(reader: BufferedReader): Iterator[String] = {
      new Iterator[String] {
        def hasNext: Boolean = reader.ready()

        def next(): String = reader.readLine()
      }
    }

    val tasksLogFile =
      CoreLauncher
        .instanceDir(clusterInstanceID)
        .resolve("log")
        .resolve("tasks.log")

    eventually(timeout(2.minutes), interval(3.seconds)) {
      val events = Seq.newBuilder[JSValue]
      Using(new BufferedReader(new FileReader(tasksLogFile.toFile))) { reader =>
        val iter = lineIterator(reader)
        while (iter.hasNext) {
          val line = iter.next()
          val jsonLine = JSON.parse[JSValue](line.getBytes)
          val loggedGIDPath = (jsonLine / "global_id_path").as[Seq[String]]
          if (loggedGIDPath == globalIDPath) {
            events.addOne(jsonLine)
          }
        }
      }.get
      val logEvents = events.result()
      logEvents.size shouldEqual expected
    }
  }
}
