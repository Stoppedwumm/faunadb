package fauna.api.test

import fauna.codex.json.JSObject
import org.scalactic.source.Position

class FeedSpecHelpers extends FQL2APISpec with EventAssertions {
  protected def poll(token: String, key: Key, params: JSObject = JSObject.empty)(
    implicit pos: Position): JSObject = {
    val res =
      client.api.post(
        "/feed/1",
        JSObject("token" -> token) ++ params,
        key.toString
      )
    res should respond(OK)
    res.json
  }
}
