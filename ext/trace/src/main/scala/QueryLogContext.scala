package fauna.trace

import io.netty.util.AsciiString
import org.parboiled2._
import scala.collection.immutable.SeqMap
import scala.util.{ Failure, Success }

case class QueryLogContext(trace: TraceContext, tags: QueryLogTags)

object QueryLogTags {
  def empty = QueryLogTags(SeqMap.empty)
  def fromHTTPHeader(
    tags: Option[String],
    maxHeaderBytes: Int = 3000,
    maxTags: Int = 25,
    maxKeyBytes: Int = 40,
    maxValueBytes: Int = 80): Either[String, QueryLogTags] = {
    // Parse key values like key1=val1,key2=val2
    // Return an error string for any invalid tags.
    val tagsMap: Either[String, SeqMap[String, String]] = tags map { tags =>
      if (tags.length > maxHeaderBytes) {
        Left("header is too long")
      } else if (tags == "") {
        Right(SeqMap.empty[String, String])
      } else {
        new TagsParser(tags).Tags.run() match {
          case Success(res) =>
            if (1 + res.tail.head.size > maxTags) {
              Left("too many tags")
            } else {
              (Seq(res.head) ++ res.tail.head)
                .foldRight[Either[String, SeqMap[String, String]]](
                  Right(SeqMap.empty)) {
                  case ((k, v), Right(map)) =>
                    if (k.length > maxKeyBytes) {
                      Left("invalid key")
                    } else if (v.length > maxValueBytes) {
                      Left("invalid value")
                    } else if (map.contains(k)) {
                      Left("duplicate key")
                    } else {
                      Right(map + (k -> v))
                    }
                  case (_, Left(error)) => Left(error)
                }
            }
          case Failure(_) => Left("invalid tags")
        }
      }
    } getOrElse Right(SeqMap.empty)

    tagsMap map { tagsMap => new QueryLogTags(tagsMap) }
  }
}

class TagsParser(val input: ParserInput) extends Parser {
  def Tags = rule { Pair ~ zeroOrMore("," ~ Pair) ~ EOI }
  def Pair: Rule1[(String, String)] = rule {
    capture(Key) ~ "=" ~ capture(Value) ~> ((a: String, b: String) => (a, b))
  }
  def Key = rule { oneOrMore(CharPredicate.AlphaNum | "_") }
  def Value = rule { oneOrMore(CharPredicate.AlphaNum | "_") }
}

/** A query log context contains the trace context the client requested, and
  * any extra tags the client requested (through the x-fauna-tags header).
  */
final case class QueryLogTags(tags: SeqMap[String, String]) {

  def toHTTPHeader: AsciiString = new AsciiString(
    tags.map(v => s"${v._1}=${v._2}").mkString(","))

  override def toString = s"QueryLogTags(tags = $tags)"
}
