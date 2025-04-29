package fauna.api.test

import fauna.codex.json._
import fauna.lang.syntax._
import fauna.net.http.HttpResponse
import fauna.prop.api._
import fauna.prop._
import org.scalatest.matchers.{ MatchResult, Matcher }
import scala.concurrent.Future

trait HttpSpecMatchers extends ResponseCodes {
  def matchJSON(expected: JSValue) = new JSONEqualsMatcher(expected)

  def containJSON(subset: JSObject) = new JSONSupersetMatcher(subset)

  def containKeys(keys: String*) = new JSONKeyMatcher(keys.toSet)

  def excludeKeys(keys: String*) = new JSONKeyMatcher(keys.toSet, true)

  def matchJSONPattern(patterns: (String, String)*) = new JSONPatternMatcher(
    Map(patterns: _*))

  def containElem(elem: JSValue) = new JSONContainsElemMatcher(elem)

  def respond(code: Int*) = new StatusCodeMatcher(code.toSet)
}

trait APISpecMatchers extends HttpSpecMatchers {

  def beResourceOfFaunaClass(className: String) =
    matchJSONPattern("ref" -> (s"""$className/.+"""), "ts" -> """\d+""")

  def containEvent(eventType: String, ref: JSValue) = new JSONContainsEventMatcher(
    (ref, eventType))

  def containEvent(eventType: String, ref: String) = new JSONContainsEventMatcher(
    (JSObject("@ref" -> ref), eventType))

  def containEvent(eventType: String, ref: JSValue, ts: Long) =
    new JSONContainsStrictEventMatcher((ref, ts, eventType))

  def containEvent(eventType: String, ref: String, ts: Long) =
    new JSONContainsStrictEventMatcher((JSObject("@ref" -> ref), ts, eventType))

  def containResource(ref: String) = new SetResourceMatcher(ref)
}

class JSONContainsEventMatcher(val expected: (JSValue, String)) extends JSONMatcher[(JSValue, String)] {
  val failure = "did not contain"
  val negatedFailure = "contained"

  def test(left: JSValue, right: (JSValue, String)) = {
    val (resource, action) = right
      left.as[JSArray].value exists { js =>
        ((js / "action").as[String] == action) && ((js / "resource").as[JSObject] == resource)
      }
  }
}

class JSONContainsStrictEventMatcher(val expected: (JSValue, Long, String)) extends JSONMatcher[(JSValue, Long, String)] {
  val failure = "did not contain"
  val negatedFailure = "contained"

  def test(left: JSValue, right: (JSValue, Long, String)) = {
    val (resource, ts, action) = right
      left.as[JSArray].value exists { js =>
        ((js / "action").as[String] == action) && ((js / "resource").as[JSObject] == resource) && ((js / "ts").as[Long] == ts)
      }
  }
}

class JSONContainsElemMatcher(val expected: JSValue) extends JSONMatcher[JSValue] {
  val failure = "did not contain"
  val negatedFailure = "contained"

  def test(left: JSValue, right: JSValue) = left.as[JSArray].value contains right
}

abstract class JSONMatcher[T] extends Matcher[JSValue] with APIResponseHelpers {
  val expected: T

  val failure: String
  val negatedFailure: String

  def normalized[A](value: A): A = {
    def normalized0[B](value: B, depth: Int): B = value match {
      case obj: JSObject =>
        val unwrapped = obj.get("@obj").asOpt[JSObject]
        val normalize = unwrapped.getOrElse(obj)
        val fields = normalize.value collect {
          case (key, value) if key != "ts" || depth > 0 =>
            (key, normalized0(value, depth + 1))
        }
        val sorted = fields sortBy { _._1 }
        JSObject(sorted: _*).asInstanceOf[B]

      case arr: JSArray =>
        val elems = arr.value map { normalized0(_, depth + 1) }
        JSArray(elems: _*).asInstanceOf[B]

      case other => other
    }

    normalized0(value, 0)
  }

  def test(actual: JSValue, expected: T): Boolean

  def apply(actual: JSValue) = {
    val a = normalized(actual)
    val e = normalized(expected)

    val aPretty = a
    val ePretty = e match {
      case e: JSObject => s"\n$e\n  "
      case e: JSArray  => s"\n$e\n  "
      case _           => e
    }

    MatchResult(
      test(a, e),
      s"$aPretty\n  $failure $ePretty",
      s"$aPretty\n  $negatedFailure $ePretty",
      s"$aPretty\n  $failure $ePretty",
      s"$aPretty\n  $negatedFailure $ePretty")
  }
}

class JSONEqualsMatcher(val expected: JSValue) extends JSONMatcher[JSValue] {

  val failure = "did not match"
  val negatedFailure = "matched"

  def test(left: JSValue, right: JSValue) = left == right
}

class JSONSupersetMatcher(val expected: JSObject) extends JSONMatcher[JSObject] {
  val failure = "did not contain the elements of"
  val negatedFailure = "contained the elements of"

  def test(sup: JSValue, sub: JSObject) = (sub -- sup.as[JSObject]) == JSObject.empty
}

class JSONKeyMatcher(val expected: Set[String], val inverted: Boolean = false) extends JSONMatcher[Set[String]] {
  val failure = if (inverted) "contained some of keys" else "did not contain all keys"
  val negatedFailure = if (inverted) "did not contain all keys" else "contained some of keys"

  override def normalized[A](a: A) = a

  def test(json: JSValue, expectedKeys: Set[String]) = {
    val keys = json.as[Map[String, JSValue]].keySet

    if (inverted) {
      (expectedKeys -- keys).size == expectedKeys.size
    } else {
      (expectedKeys -- keys).isEmpty
    }
  }
}

class JSONPatternMatcher(val expected: Map[String, String]) extends JSONMatcher[Map[String, String]] {

  val failure = "did not match resource pattern"
  val negatedFailure = "matched resource pattern"

  override def normalized[A](a: A) = a

  def test(json: JSValue, patterns: Map[String, String]) = {
    val failedPatterns = patterns map { case (k, v) => k -> v.r } filterNot {
      case (field, regex) => (json / field).asOpt[JSValue] flatMap {
        case JSArray(_)    => None
        case JSObject(_)   => None
        case JSString(str) => Some(str)
        case v             => Some(v.toString.trim)
      } map {
        regex.unapplySeq(_).isDefined
      } getOrElse {
        false
      }
    }

    failedPatterns.isEmpty
  }
}

class StatusCodeMatcher(val codes: Set[Int]) extends Matcher[Future[HttpResponse]]
    with APIResponseHelpers {

  def msg(code: Int) = code match {
    case 200  => "200 OK"
    case 201  => "201 Created"
    case 204  => "204 No Content"
    case 401  => "401 Not Authorized"
    case 404  => "404 Not Found"
    case 500  => "500 Internal server error"
    case code => code.toString
  }

  def apply(res: Future[HttpResponse]) = {
    val messages = codes.toSeq.sorted map { msg(_) }
    val expected = if (messages.size == 1) messages.head else s"one of ${messages mkString ", "}"
    val failure = s"got statusCode ${msg(res.statusCode)}, expected $expected ${res.body.toUTF8String}"
    val negatedFailure = s"got statusCode ${msg(res.statusCode)}, expected not $expected ${res.body.toUTF8String}"

    MatchResult(codes contains res.statusCode, failure, negatedFailure, failure, negatedFailure)
  }
}

class BooleanMatcher(val expected: Boolean, val key: String) extends Matcher[Future[HttpResponse]]
    with APIResponseHelpers {

  val failure = (if (expected) "%s was not %s" else "%s was %s").format(key, expected)
  val negatedFailure = (if (expected) "%s was %s" else "%s was not %s").format(key, expected)

  def apply(res: Future[HttpResponse]) = MatchResult(
    res.resource[Boolean](key) == expected,
    failure, negatedFailure, failure, negatedFailure)
}

// FIXME Should maybe merge these event matches together

class EventSetEventMatcher(val eventType: String, val ref: String) extends Matcher[Future[HttpResponse]]
    with APIResponseHelpers {

  val failure = "%s did not contain event (%s, %s)"
  val negatedFailure = "%s contained event (%s, %s)"

  def apply(res: Future[HttpResponse]) = MatchResult(
    res.events exists { e => (e / "action").asOpt[String].contains(eventType) && (e / "resource").asOpt[String].contains(ref) },
    failure.format(res.events, eventType, ref),
    negatedFailure.format(res.events, eventType, ref),
    failure.format(res.events, eventType, ref),
    negatedFailure.format(res.events, eventType, ref))
}

class EventSetExactEventMatcher(val eventType: String, val ref: String, val ts: Long) extends Matcher[Future[HttpResponse]]
    with APIResponseHelpers {

  val failure = "%s did not contain event (%s, %s, %d)"
  val negatedFailure = "%s contained event (%s, %s, %d)"

  def apply(res: Future[HttpResponse]) = MatchResult(
    res.events exists { e => (e / "action").asOpt[String].contains(eventType) && (e / "resource").asOpt[String].contains(ref) && (e / "ts").asOpt[Long].contains(ts) },
    failure.format(res.events, eventType, ref, ts),
    negatedFailure.format(res.events, eventType, ref, ts),
    failure.format(res.events, eventType, ref, ts),
    negatedFailure.format(res.events, eventType, ref, ts))
}

class SetResourceMatcher(val ref: String) extends Matcher[Future[HttpResponse]]
    with APIResponseHelpers {

  val failure = "%s did not contain ref '%s'"
  val negatedFailure = "%s contained ref '%s'"

  def apply(res: Future[HttpResponse]) = MatchResult(
    res.resources contains ref,
    failure.format(res.resources, ref),
    negatedFailure.format(res.resources, ref),
    failure.format(res.resources, ref),
    negatedFailure.format(res.resources, ref))
}
