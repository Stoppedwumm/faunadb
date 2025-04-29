package fauna.model.runtime.fql2.serialization

import fauna.codex.json2._
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ QueryCheckFailure, Result }
import fauna.model.runtime.fql2.FQLInterpCtx
import fauna.repo.query.Query
import fauna.repo.values.Value
import fql.ast.{ Displayer, Expr, Literal, Span, Src }
import fql.error.ParseError
import fql.parser.{ Parser, TemplateSigil }
import scala.collection.immutable.{ ArraySeq, SeqMap }
import scala.collection.mutable.{ ArrayBuffer, SeqMap => MSeqMap }

final case class FQL2Query(
  queryString: String,
  // a sequence of values and offset positions to their placeholders in `queryString`
  values: ArrayBuffer[(UndecidedValue, Int)] = ArrayBuffer.empty) {

  def parse(ctx: FQLInterpCtx): Query[Result[(Map[String, Value], Expr)]] = {
    val parsedQ: Query[Result[Expr]] =
      Query.value(Parser.queryTemplate(queryString))

    val argsQ = values.iterator.zipWithIndex
      .map { case ((undecided, qidx), ord) =>
        undecided.toValue(ctx, ValueFormat.Tagged).map {
          case Right(v) => Result.Ok(("$" + ord) -> v)
          case Left(e)  => Result.Err(valueFailure(e, qidx))
        }
      }
      .toSeq
      .sequenceT
      .mapT(_.toMap)

    (parsedQ, argsQ).parT { (expr, args) =>
      Query.value(Result.Ok((args, expr)))
    }
  }

  private def valueFailure(e: InvalidValue, index: Int) = {
    val src = Src.Query(queryString)
    val span = Span(index, index + TemplateSigil.Value.length, src)
    QueryCheckFailure(Seq(ParseError(e.message, span)))
  }
}

object FQL2Query {

  implicit object JSONDecoder
      extends JSON.PartialDecoder[FQL2Query]("query template") {
    override def readString(str: String, stream: JSON.In) =
      FQL2Query(str, ArrayBuffer.empty)

    override def readObjectStart(stream: JSON.In) = {
      val b = new JSONBuilder
      b.readObjectStart(stream)
      b.result()
    }
  }

  /** JSON parser for query template object. Builds the query string and list of values.
    */
  private final class JSONBuilder extends PartialJSONSwitch[Unit]("query template") {
    private val query = new StringBuilder
    private val values = ArrayBuffer.empty[(UndecidedValue, Int)]
    // true if the wrapping node is an object/array. If we're in a container
    // node, then we wrap the fql node's output in parens in order to preserve
    // lexical scope.
    private var inContainerNode = false

    def result() = FQL2Query(query.result(), values)

    // helpers for determining if "array" or "object" forms can be replaced with
    // pure values. When this is possible it saves on eval and logging costs
    // down the line.

    private var hitQuery = false
    private def checkHitQuery[T](f: => T): T = {
      // The reentrant behavior is a bit tricky: It's possible for e.g. "array"
      // to be nested, e.g.: { "array": [ Q, { "array": [V, V] }, Q ] }.
      //
      // The inner array can be replaced with an array value, but the outer
      // cannot, since it also contains at least one query. Therefore when
      // entering this method, we turn hitQuery off, so the inner container has
      // a chance to be value-converted.
      //
      // The inverse: { "array": [ V, { "array": [Q, Q] }, V ] }
      //
      // If an inner container cannot be value-converted, neither can the outer
      // container, so the prev state is ORed with the inner state before
      // returning.
      val prev = hitQuery
      hitQuery = false
      val rv = f
      hitQuery ||= prev
      rv
    }

    private def replaceWithValue(qstart: Int, vstart: Int, uv: UndecidedValue) = {
      // reset the query and append a value sigil
      query.length = qstart
      query.append(TemplateSigil.Value)
      // reset values and push the replacement
      values.takeInPlace(vstart)
      values += (uv -> qstart)
    }

    // JSONSwitch impl

    override def readString(str: String, stream: JSON.In) = {
      if (str.contains(TemplateSigil.Nul)) {
        throw new JSONParseException("FQL strings cannot contain NUL characters")
      }
      hitQuery = true

      if (inContainerNode) query.append('(')
      query.append(str)
      if (inContainerNode) query.append(')')
    }

    override def readObjectStart(stream: JSON.In) = {
      def error = throw JSONUnexpectedTypeException(
        JSONTypes.ObjectFieldNameLabel,
        "an object with a single key 'fql', 'value', 'array', or 'object'")

      stream.read(JSONParser.ObjectFieldNameSwitch) match {
        case "fql" =>
          val inCont = inContainerNode
          inContainerNode = false
          decodeTemplate(stream, wrapParens = inCont)
          inContainerNode = inCont
        case "value" => decodeValue(stream)
        case "array" =>
          val inCont = inContainerNode
          inContainerNode = true
          decodeArray(stream)
          inContainerNode = inCont
        case "object" =>
          val inCont = inContainerNode
          inContainerNode = true
          decodeObject(stream)
          inContainerNode = inCont
        case _ => error
      }

      if (!stream.skipObjectEnd) {
        error
      }
    }

    // Decoders for template variants. These directly decode JSON into the query
    // buffer and values builder.

    private def decodeTemplate(stream: JSON.In, wrapParens: Boolean): Unit = {
      stream.read(JSONParser.ArrayStartSwitch)

      if (wrapParens) query.append('(')

      while (!stream.skipArrayEnd) {
        stream.read(this)
      }

      if (wrapParens) query.append(')')
    }

    private def decodeValue(stream: JSON.In): Unit = {
      values.append(JSON.decode[UndecidedValue](stream) -> query.length)
      query.append(TemplateSigil.Value)
    }

    private def decodeArray(stream: JSON.In): Unit = {
      stream.read(JSONParser.ArrayStartSwitch)

      checkHitQuery {
        val qstart = query.length
        val vstart = values.length

        query.append("[")
        var init = true
        while (!stream.skipArrayEnd) {
          if (init) init = false else query.append(", ")
          stream.read(this)
        }
        query.append("]")

        if (!hitQuery) {
          // we can convert this array into a pure value.
          val res = values.view.drop(vstart).map(_._1).to(ArraySeq)
          replaceWithValue(qstart, vstart, UndecidedValue.Array(res))
        }
      }
    }

    private def decodeObject(stream: JSON.In): Unit = {
      stream.read(JSONParser.ObjectStartSwitch)

      checkHitQuery {
        val qstart = query.length
        val vstart = values.length
        val keys = MSeqMap.empty[String, Unit]

        // Use a displayer to write field names
        val qdisplay = new Displayer(query)
        query.append("{ ")
        var init = true
        while (!stream.skipObjectEnd) {
          if (init) init = false else query.append(", ")
          val k = stream.read(JSONParser.ObjectFieldNameSwitch)
          if (keys.contains(k)) {
            throw JSONParseException(s"Duplicate object field `$k`")
          }
          keys += (k -> ())
          qdisplay.writeLit(Literal.Str(k))
          query.append(": ")
          stream.read(this)
        }
        query.append(" }")

        if (!hitQuery) {
          // we can convert this object into a pure value.
          val res = keys.view
            .zip(values.view.drop(vstart))
            .map { case ((k, _), (v, _)) => (k, v) }
            .to(SeqMap)

          // Wrap the object in an object tag, to handle fields that start with `@`
          // correctly.
          //
          // NB: This is reliable because we always use `ValueFormat.Tagged` when
          // deserializing this undecided value.
          val taggedObject = UndecidedValue.Object(
            SeqMap(ValueFormat.ObjectTag -> UndecidedValue.Object(res)))

          replaceWithValue(qstart, vstart, taggedObject)
        }
      }
    }
  }
}
