package fauna.qa.generators.test

import fauna.codex.json._
import fauna.prop._
import fauna.prop.api._
import fauna.qa._
import java.util.concurrent.ConcurrentSkipListSet

class FqlxSimpleCRUDGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val collName = "fqlxCRUD"
  val indexName = "docs_by_indexed_field"
  val indexedFieldName = "indexed_field"

  val schema = Schema.DB(name)

  override protected def initializer(schema: Schema.DB): RequestIterator =
    RequestIterator(
      "initFqlxCollection", 
      Seq(
        FaunaQuery(
          schema.serverKey.get,
          JSObject("query" -> JSString(s"""Collection.create(
            | {
            |   name: "$collName",
            |   constraints: [
            |     { unique: [ "$indexedFieldName" ] }
            |   ],
            |   indexes: {
            |     $indexName: {
            |       terms: [
            |         {
            |           field: "$indexedFieldName"
            |         }
            |       ]
            |     }
            |   }
            | })""".stripMargin)),
          requestPathOverride = Some("/query/1")
        )
      )
    )

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get

    new RequestStream {
      override val name: String = "crudOps"
      override def next(): FaunaQuery =
        docFieldValues.size() match {
          case length if length < 10 => create(auth)
          case _ =>
            val func = funcProp.sample
            func(auth)
        }
    }
  }

  var docFieldValues = new ConcurrentSkipListSet[String]()

  val fieldProp = Prop.alphaNumString()

  val funcSeq: Seq[String => FaunaQuery] = Seq(create, read, update, delete)
  val funcProp = Prop.choose(funcSeq)

  def create(auth: String) = {
    val value = fieldProp sample

    docFieldValues.add(value)

    FaunaQuery(
      auth,
      JSObject(
        "query" ->
          JSString(s"""$collName.create({ $indexedFieldName: "$value" })""")),
      requestPathOverride = Some("/query/1")
    )
  }

  def read(auth: String) = {
    val value = Prop.choose(docFieldValues.toArray()) sample

    FaunaQuery(
      auth,
      JSObject(
        "query" -> JSString(
          s"""let doc = $collName.$indexName("$value")
          | doc.$indexedFieldName""".stripMargin
        )),
      requestPathOverride = Some("/query/1")
    )
  }

  def update(auth: String) = {
    val value = Prop.choose(docFieldValues.toArray()) sample

    FaunaQuery(
      auth,
      JSObject(
        "query" -> JSString(
          s"""let doc = $collName.$indexName("$value").first()
          | doc.update({ $indexedFieldName: "$value" })""".stripMargin
        )),
      requestPathOverride = Some("/query/1")
    )
  }

  def delete(auth: String) = {
    val value = Prop.choose(docFieldValues.toArray()) sample

    docFieldValues.remove(value)

    FaunaQuery(
      auth,
      JSObject(
        "query" -> JSString(
          s"""let doc = $collName.$indexName("$value").first()
          | doc.delete()""".stripMargin
        )),
      requestPathOverride = Some("/query/1")
    )
  }
}

class FqlxSimpleCRUD(config: QAConfig) extends QATest {
  val generators: Array[TestGenerator] = Array(
    new FqlxSimpleCRUDGenerator("fqlxSimpleCRUD", config))
}
