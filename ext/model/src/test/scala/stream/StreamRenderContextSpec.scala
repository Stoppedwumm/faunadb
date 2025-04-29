package fauna.model.test

import fauna.atoms._
import fauna.auth._
import fauna.codex.json._
import fauna.codex.json2._
import fauna.exec._
import fauna.lang._
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.model.stream._
import fauna.repo.test.CassandraHelper
import fauna.snowflake.IDSource
import fauna.stats._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.ops._
import io.netty.buffer.ByteBuf

class StreamRenderContextSpec extends Spec {
  import FaunaExecutionContext.Implicits.global
  import SocialHelpers._

  val ctx = CassandraHelper.context("model")
  val scope = ctx ! newScope
  val db = (ctx ! Database.forScope(scope)).get
  val auth = Auth.forScope(scope)

  def render(auth: Auth, stream: Observable[TxnEvents]): Observable[ByteBuf] =
    new StreamRenderContext(
      repo = ctx,
      stats = StatsRecorder.Null,
      idSource = new IDSource(() => 42),
      auth = auth,
      version = APIVersion.Default,
      fields = EventField.Defaults,
      streamID = StreamID(1234),
      stream = stream
    ).render()

  "StreamRenderContextSpec" - {

    "should render events as JSON" in {
      val source =
        Observable.single(
          TxnEvents(
            Timestamp.Epoch,
            Clock.time,
            Vector(StreamStart(Timestamp.Epoch))
          ))

      val events =
        await(render(auth, source).sequenceF) collect { buf =>
          JSON.parse[JSValue](buf)
        }

      events should contain only
        JSObject(
          "type" -> "start",
          "txn" -> 0,
          "event" -> 0
        )
    }

    "should filter events" in {
      val source =
        Observable.single(
          TxnEvents(
            Timestamp.Epoch,
            Clock.time,
            Vector(
              NewVersionAdded(
                VersionAdd(
                  ScopeID.RootID,
                  DocID(SubID(1), CollectionID(1234)),
                  Unresolved,
                  Create,
                  SchemaVersion.Min,
                  Data.empty,
                  diff = None
                )))))

      val stream = render(auth.withPermissions(NullPermissions), source)
      await(stream.sequenceF) shouldBe empty // no events came
    }

    "should close the stream on invalid auth" in {
      val (pub, source) = Observable.gathering(OverflowStrategy.unbounded[TxnEvents])
      val (secret, key) = ctx ! mkKey(db.name)
      val stream = render(Auth.forScope(scope, Some(key)), source)

      ctx ! runQuery(RootAuth, DeleteF(Select("ref", KeyFromSecret(secret))))

      pub.publish(
        TxnEvents(
          Timestamp.Epoch,
          Clock.time,
          Vector(StreamStart(Timestamp.Epoch))
        ))

      val events =
        await(stream.sequenceF) collect { buf =>
          JSON.parse[JSValue](buf)
        }

      events should contain only
        JSObject(
          "type" -> "error",
          "txn" -> 0,
          "event" -> JSObject(
            "code" -> "permission denied",
            "description" -> "Authorization lost during stream evaluation."
          )
        )
    }
  }
}
