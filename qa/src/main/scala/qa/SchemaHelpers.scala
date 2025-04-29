package fauna.qa

import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp

sealed trait Schema

object Schema {

  case class DB(
    name: String,
    priority: Option[Int] = None,
    schema: Vector[Schema] = Vector.empty,
    adminKey: Option[String] = None,
    serverKey: Option[String] = None,
    auth0JWT: Option[String] = None,
    var lastSeenTxnTimestamp: Timestamp = Timestamp.Epoch
  ) extends Schema {

    override def toString =
      s"DB(name = $name, " +
        s"priority = $priority, " +
        s"schema = $schema, " +
        s"admin = $adminKey, " +
        s"server = $serverKey, " +
        s"auth0 = $auth0JWT, " +
        s"lastSeenTxnTime = $lastSeenTxnTimestamp)"
  }

  object DB {

    def Root(rootKey: String) =
      DB("__ROOT__", None, Vector.empty, Some(rootKey), None)

    def apply(name: String, schema: Schema*): DB =
      DB(name, None, schema.toVector)

    def apply(name: String, priority: Int, schema: Schema*): DB =
      DB(name, Some(priority), schema.toVector)

    implicit val Codec: CBOR.Codec[DB] = CBOR.RecordCodec[DB]
  }

  case class Collection(name: String, historyDays: Option[Long] = None)
      extends Schema

  object Collection {
    implicit val Codec: CBOR.Codec[Collection] = CBOR.RecordCodec[Collection]
  }

  case class Index(
    name: String,
    source: String,
    terms: Vector[Schema.Index.Term],
    values: Vector[Schema.Index.Value],
    unique: Boolean
  ) extends Schema

  object Index {

    def apply(name: String, source: String, term: Vector[String]): Index =
      Index(name, source, Vector(Term(term)), Vector.empty, false)

    def apply(
      name: String,
      source: String,
      term: Vector[String],
      value: Vector[String]
    ): Index =
      Index(name, source, Vector(Term(term)), Vector(Value(value)), false)

    case class Term(field: Vector[String], transform: Option[String] = None)

    case class Value(
      field: Vector[String],
      reverse: Boolean = false,
      transform: Option[String] = None
    )

    implicit val TermCodec = CBOR.RecordCodec[Term]
    implicit val ValueCodec = CBOR.RecordCodec[Value]
    implicit val Codec: CBOR.Codec[Index] = CBOR.RecordCodec[Index]
  }

  implicit val Codec: CBOR.Codec[Schema] =
    CBOR.SumCodec[Schema](
      DB.Codec,
      Collection.Codec,
      Index.Codec
    )
}
