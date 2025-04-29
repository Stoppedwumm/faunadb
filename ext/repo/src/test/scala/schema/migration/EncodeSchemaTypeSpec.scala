package fauna.repo.test

import fauna.atoms._
import fauna.repo.schema._
import fauna.repo.schema.migration._
import fauna.repo.schema.ScalarType._
import fauna.repo.schema.SchemaType._
import fauna.storage.ir._
import org.scalactic.source.Position

class EncodeSchemaTypeSpec extends Spec {
  import IRValue._
  import SchemaTypeCodec._

  def roundtripType(ty: SchemaType, v: IRValue)(implicit pos: Position): Unit = {
    ty.encode shouldBe v
    SchemaTypeCodec.decode(v) shouldBe ty
  }

  "it encodes scalars" in {
    roundtripType(Any, "Any")
    roundtripType(Str, "Str")
    roundtripType(Null, "Null")
    roundtripType(Int, "Int")
    roundtripType(Long, "Long")
    roundtripType(Double, "Double")
    roundtripType(Number, "Number")
    roundtripType(Boolean, "Boolean")
    roundtripType(Str, "Str")
    roundtripType(Time, "Time")
    roundtripType(Date, "Date")
    roundtripType(Bytes, "Bytes")
    roundtripType(UUID, "UUID")
    roundtripType(V4Query, "V4Query")
    roundtripType(AnyDoc, "AnyDoc")

    roundtripType(
      ScalarType.DocType(CollectionID(1234), "Document"),
      MapV(
        "type" -> "Doc",
        "coll" -> DocIDV(DocID(SubID(1234), CollectionID(1)))
      ))
    roundtripType(
      ScalarType.NullDocType(CollectionID(1234), "Document"),
      MapV(
        "type" -> "NullDoc",
        "coll" -> DocIDV(DocID(SubID(1234), CollectionID(1)))
      ))
  }

  "it encodes structs" in {
    roundtripType(
      Record("foo" -> Str),
      MapV(
        "type" -> "Struct",
        "fields" -> MapV(
          "foo" -> "Str"
        )
      ))

    roundtripType(
      Record("foo" -> Str, "bar" -> Union(Str, Int)),
      MapV(
        "type" -> "Struct",
        "fields" -> MapV(
          "foo" -> "Str",
          "bar" -> MapV("type" -> "Union", "elems" -> ArrayV("Str", "Int"))
        ))
    )

    roundtripType(
      ObjectType(
        StructSchema(
          Map("foo" -> Str, "bar" -> Union(Str, Int)),
          wildcard = Some(Union(Str, Int)))),
      MapV(
        "type" -> "Struct",
        "fields" -> MapV(
          "foo" -> "Str",
          "bar" -> MapV("type" -> "Union", "elems" -> ArrayV("Str", "Int"))
        ),
        "wildcard" -> MapV("type" -> "Union", "elems" -> ArrayV("Str", "Int"))
      )
    )
  }

  "it encodes arrays" in {
    roundtripType(
      Array(Str),
      MapV(
        "type" -> "Array",
        "elem" -> "Str"
      ))

    roundtripType(
      Array(Union(Int, Str)),
      MapV(
        "type" -> "Array",
        "elem" -> MapV("type" -> "Union", "elems" -> ArrayV("Int", "Str"))
      ))
  }

  "it encodes tuples" in {
    roundtripType(
      Tuple(Str, Int),
      MapV(
        "type" -> "Tuple",
        "elems" -> ArrayV("Str", "Int")
      ))

    roundtripType(
      Union(Tuple(), Tuple(Str), Tuple(Str, Union(Str, Int))),
      MapV(
        "type" -> "Union",
        "elems" -> ArrayV(
          MapV("type" -> "Tuple", "elems" -> ArrayV()),
          MapV("type" -> "Tuple", "elems" -> ArrayV("Str")),
          MapV(
            "type" -> "Tuple",
            "elems" -> ArrayV(
              "Str",
              MapV("type" -> "Union", "elems" -> ArrayV("Str", "Int"))))
        )
      )
    )
  }
}
