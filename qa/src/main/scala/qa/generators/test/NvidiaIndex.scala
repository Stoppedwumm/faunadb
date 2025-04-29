package fauna.qa.generators.test

import fauna.codex.json.{ JSArray, JSObject, JSValue }
import fauna.qa._
import fauna.qa.generators.test.nvidiaindex.{ TestConfig, TestData }
import java.util.concurrent.LinkedBlockingQueue
import java.util.Queue
import scala.concurrent.duration._
import scala.util.Random

/**
  * Simulate NVIDIA's workload
  */
case class IndexSpec(streamName: String, termExtractor: (JSValue) => JSValue) {
  val indexName = s"user_tokens_by_$streamName"
}

object NvidiaIndexGenerator {
  val maxTries = 5
  val retryDelay = 3.seconds
}

class NvidiaIndexGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config) {
  val tConfig = new TestConfig(config)

  val schema = Schema.DB(
    name,
    Schema.Collection("user_tokens"),
    Schema.Index(
      "user_tokens_by_token",
      "user_tokens",
      Vector(Schema.Index.Term(Vector("data", "token"))),
      Vector.empty,
      true
    ),
    Schema.Index(
      "user_tokens_by_scope",
      "user_tokens",
      Vector(
        Schema.Index.Term(Vector("data", "id")),
        Schema.Index.Term(Vector("data", "deviceId"))
      ),
      Vector.empty,
      true
    ),
    Schema.Index(
      "user_tokens_by_user",
      "user_tokens",
      Vector(Schema.Index.Term(Vector("data", "id"))),
      Vector.empty,
      false
    )
  )

  private val mqbF = makeQueryBody(_, _)
  private val mqbwlF = makeQueryBodyWithLambda(_, _)

  def stream(schema: Schema.DB) = {
    val qbms = Seq(mqbF, mqbwlF)

    val indexSpecs = Seq(
      IndexSpec("token", { o =>
        o / "token"
      }),
      IndexSpec("scope", { o =>
        JSArray(o / "id", o / "deviceId")
      }),
      IndexSpec("user", { o =>
        o / "id"
      })
    )

    val it = for {
      is <- indexSpecs.permutations
      qbm1 <- qbms
      qbm2 <- qbms
      qbm3 <- qbms
    } yield createTokens(schema, is, Seq(qbm1, qbm2, qbm3))

    new MultiStream(it.toIndexedSeq)
  }

  private var testData: Option[TestData] = None

  def data(auth: String) =
    testData match {
      case Some(td) => td
      case None =>
        val td = new TestData(auth, propConfig, tConfig)
        testData = Some(td)
        td
    }

  private type QueryBodyMakerF = (String, JSValue) => JSObject
  private type FaunaQueryGenF = () => Option[FaunaQuery]

  private def createTokens(
    schema: Schema.DB,
    indexSpecs: Seq[IndexSpec],
    queryBodyMakers: Seq[QueryBodyMakerF]
  ) = {
    val retryQueue = new LinkedBlockingQueue[FaunaQuery]()
    val random = new Random()

    RequestStream(
      getTokenStreamName(indexSpecs, queryBodyMakers), { () =>
        Option(retryQueue.poll()) match {
          case Some(query) => query
          case None =>
            val auth = schema.serverKey.get

            val userToken = data(auth).userToken.sample
            val userTokenData = userToken.data.get / "data"

            // Create new userToken object
            new FaunaQuery(auth, userToken.toRequest.query) {
              override def result(body: JSObject): Option[FaunaQuery] = {
                // Perform queries on indices that should return the newly created userToken object
                val q =
                  (indexSpecs zip queryBodyMakers).foldRight[FaunaQueryGenF](() =>
                    None
                  )({ (im, fn) => () =>
                    makeQueryMustBeNonEmpty(
                      auth,
                      im,
                      userTokenData,
                      fn,
                      random,
                      retryQueue
                    )
                  })
                q()
              }
            }
        }
      }
    )
  }

  private def getTokenStreamName(
    indexSpecs: Seq[IndexSpec],
    queryBodyMakers: Seq[QueryBodyMakerF]
  ) = {
    val sb = new StringBuilder()
    indexSpecs foreach { s =>
      sb.append(s.streamName).append("-")
    }
    sb.append("-")
    queryBodyMakers foreach { m =>
      sb.append(if (m eq mqbF) "o" else "l")
    }
    sb.toString
  }

  private def makeQueryBody(index: String, terms: JSValue) =
    Paginate(Match(Ref(s"indexes/$index"), terms))

  private def makeQueryBodyWithLambda(index: String, terms: JSValue) =
    MapF(
      Lambda("x" -> Get(Var("x"))),
      makeQueryBody(index, terms)
    )

  private def makeQueryMustBeNonEmpty(
    auth: String,
    indexSpecAndMaker: (IndexSpec, QueryBodyMakerF),
    data: JSValue,
    nextQueryMaker: FaunaQueryGenF,
    random: Random,
    retryQueue: Queue[FaunaQuery]
  ) = {
    val (indexSpec, queryBodyMaker) = indexSpecAndMaker
    val indexName = indexSpec.indexName
    val terms = indexSpec.termExtractor(data)

    Some(new FaunaQuery(auth, queryBodyMaker(indexSpec.indexName, terms)) {
      @volatile var attempt = 1
      override def result(body: JSObject): Option[FaunaQuery] = {
        val firstAttempt = attempt == 1
        val a = (body / "resource" / "data").as[JSArray]
        if (a.isEmpty) {
          log.warn(s"$indexName index lookup failed for $terms, attempt=$attempt")
          if (attempt != NvidiaIndexGenerator.maxTries) {
            scheduleRetry(retryQueue)
          }
        } else if (!firstAttempt || random.nextInt(10000) == 0) {
          // We log every successful retry, and randomly some first tries
          // too just to reassure ourselves that everything works.
          log.info(s"$indexName index lookup succeeded for $terms, attempt=$attempt")
        }

        // Only chain queries on their first attempt, retries of failed queries
        // don't result in new dependent queries (as we're only retrying the
        // failed one, and not the ones that logically followed it originally).
        if (firstAttempt) {
          nextQueryMaker()
        } else {
          None
        }
      }

      private def scheduleRetry(retryQueue: Queue[FaunaQuery]) = {
        attempt = attempt + 1
        val query = this
        config.timer.scheduleTimeout(NvidiaIndexGenerator.retryDelay) {
          retryQueue.add(query)
        }
      }
    })
  }
}

class NvidiaIndex(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new NvidiaIndexGenerator("NvidiaIndex", config)
  )
}
