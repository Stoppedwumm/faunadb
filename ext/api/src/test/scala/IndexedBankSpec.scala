package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.codex.json2.JSON
import fauna.exec.Timer
import fauna.lang.syntax._
import fauna.prop.Prop
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

/**
 * The provenance of this test was an issue surfaced by the Jepsen bank-index test;
 * hence the name and structure.
 */
class IndexedBankSpec extends QueryAPI21Spec {
  val RunTime = 10.seconds

  "IndexedBank" - {
    once("always returns the correct total balance") {
      for {
        db <- aDatabase
      } {
        val globalTotal = 100

        runQuery(CreateClass(MkObject(
          "name" -> "accounts")), db)

        runQuery(CreateIndex(MkObject(
          "name" -> "all_accounts",
          "source" -> ClassRef("accounts"),
          "active" -> true,
          "partitions" -> 1,
          "values" -> JSArray(
            MkObject("field" -> JSArray("ref")),
            MkObject("field" -> JSArray("data", "balance"))))), db)

        val refs = (0 to 7) map { MkRef(ClassRef("accounts"), _) }
        val refP = Prop.int(0 to 7)

        runQuery(CreateF(refs.head, MkObject("data" -> MkObject("balance" -> globalTotal))), db)

        val amntP = Prop.int(1 to 10) map { JSLong(_) }

        def transfer(): Future[Unit] = {
          val fromI = refP.sample
          var toI = fromI
          while (toI == fromI) toI = refP.sample

          val from = refs(fromI)
          val to = refs(toI)

          val amount = amntP.sample

          val fromQ =
            Let("a" -> Subtract(
                  If(Exists(from),
                    Select(JSArray("data", "balance"), Get(from)),
                    0),
                  amount)
            ) {
              If(LessThan(Var("a"), 0),
                Abort("balance would go negative"),
                If(Equals(Var("a"), 0),
                  DeleteF(from),
                  Update(from, MkObject("data" -> MkObject("balance" -> Var("a"))))))
            }

          val toQ =
            If(Exists(to),
              Let("b" -> AddF(Select(JSArray("data", "balance"), Get(to)), amount)) {
                Update(to, MkObject("data" -> MkObject("balance" -> Var("b"))))
              },
              CreateF(to, MkObject("data" -> MkObject("balance" -> amount))))

          val q =
            Let("from" -> fromQ, "to" -> toQ) {
              JSArray(
                JSArray(
                  Select(JSArray("ts"), Var("from")),
                  Select(JSArray("ref", "id"), Var("from")),
                  Select(JSArray("data", "balance"), Var("from"))),
                JSArray(
                  Select(JSArray("ts"), Var("to")),
                  Select(JSArray("ref", "id"), Var("to")),
                  Select(JSArray("data", "balance"), Var("to"))))
            }

          runRawQuery(q, db.key) transform { _ => Success(()) }
        }

        def read(): Future[Unit] = {
          val q = MapF(
              Lambda(JSArray("ref", "bal") -> JSArray(Select("id", Var("ref")), Var("bal"))),
              Paginate(Match(IndexRef("all_accounts"))))

          runRawQuery(q, db.key) flatMap { res =>
            res.body.data flatMap { body =>
              val json = JSON.parse[JSValue](body).as[JSObject]
              val data = (json / "resource" / "data").as[JSArray].value
              val total = data.foldLeft(0L) { (acc, obj) => acc + (obj / 1).as[Long] }
              if (total != globalTotal) {
                Future.failed(new IllegalStateException(s"balance is wrong: $total != $globalTotal"))
              } else {
                Future.unit
              }
            }
          }
        }

        val opP = Prop.boolean map { if (_) read() else transfer() }

        @volatile var stop: Boolean = false
        def run(): Future[Unit] =
          if (stop) {
            Future.unit
          } else {
            opP.sample before run()
          }

        Timer.Global.scheduleTimeout(RunTime) { stop = true }

        val runners = (0 to 2) map { _ => run() }
        Await.result(Future.sequence(runners), Duration.Inf)
      }
    }
  }
}
