package fauna.repo.test

import fauna.storage.doc._
import fauna.storage.ir._
import scala.collection.immutable.Queue

sealed abstract class Item
case object NoArgs extends Item
case class SimpleArg(name: String) extends Item
case class Tagged(name: String, tag: Option[String]) extends Item
case class Override(name: String) extends Item

class FieldTypeSpec extends Spec {
  val simon = SimpleArg("simon")
  val simonData = MapV("name" -> StringV("simon"))

  val waldo = Tagged("waldo", None)
  val waldoData = MapV("name" -> StringV("waldo"))

  val fred = Tagged("fred", Some("foo"))
  val fredData = MapV("name" -> StringV("fred"), "tag" -> StringV("foo"))

  val alias = Override("jose")
  val aliasData = MapV("nombre" -> StringV("jose"))

  "RecordCodec" - {
    val SimpleArgT = FieldType.RecordCodec[SimpleArg]
    val TaggedT = FieldType.RecordCodec[Tagged]
    val OverrideT = FieldType.RecordOverrideCodec[Override]("name" -> "nombre")

    "encodes" in {
      SimpleArgT.encode(simon) should equal (Some(simonData))
      TaggedT.encode(waldo) should equal (Some(waldoData))
      TaggedT.encode(fred) should equal (Some(fredData))
      OverrideT.encode(alias) should equal (Some(aliasData))
    }

    "decodes" in {
      SimpleArgT.decode(Some(simonData), Queue.empty) should equal (Right(simon))
      TaggedT.decode(Some(waldoData), Queue.empty) should equal (Right(waldo))
      TaggedT.decode(Some(fredData), Queue.empty) should equal (Right(fred))
      OverrideT.decode(Some(aliasData), Queue.empty) should equal (Right(alias))
    }
  }

  "SumCodec" - {
    val item = Field[String]("item")

    val ItemT = FieldType.SumCodec[String, Item](item,
      "none" -> FieldType.Empty(NoArgs),
      "simple" -> FieldType.RecordCodec[SimpleArg],
      "tag" -> FieldType.RecordCodec[Tagged],
      "alias" -> FieldType.RecordOverrideCodec[Override]("name" -> "nombre"))

    "encodes" in {
      ItemT.encode(NoArgs) should equal (Some(MapV("item" -> "none")))
      ItemT.encode(simon) should equal (Some(simonData.update(item.path, "simple")))
      ItemT.encode(waldo) should equal (Some(waldoData.update(item.path, "tag")))
      ItemT.encode(fred) should equal (Some(fredData.update(item.path, "tag")))
      ItemT.encode(alias) should equal (Some(aliasData.update(item.path, "alias")))
    }

    "decodes" in {
      ItemT.decode(Some(MapV("item" -> "none")), Queue.empty) should equal (Right(NoArgs))
      ItemT.decode(Some(simonData.update(item.path, StringV("simple"))), Queue.empty) should equal (Right(simon))
      ItemT.decode(Some(waldoData.update(item.path, StringV("tag"))), Queue.empty) should equal (Right(waldo))
      ItemT.decode(Some(fredData.update(item.path, StringV("tag"))), Queue.empty) should equal (Right(fred))
      ItemT.decode(Some(aliasData.update(item.path, StringV("alias"))), Queue.empty) should equal (Right(alias))
    }

    "Nullable" - {
      val NullableT =
        new NullableFieldType[ScalarV](ScalarV.Type, identity, { case v: ScalarV => v })

      "encodes" in {
        NullableT.encode(NullV) should equal (Some(NullV))
      }

      "decodes" in {
        NullableT.decode(Some(NullV), Queue.empty) should equal (Right(NullV))
      }
    }
  }
}
