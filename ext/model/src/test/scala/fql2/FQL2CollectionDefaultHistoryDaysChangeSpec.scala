package fauna.model.test

import fauna.atoms._
import fauna.auth.Auth
import fauna.lang.clocks.Clock
import fauna.model._
import fauna.model.schema._
import fauna.storage.doc._
import scala.concurrent.duration._

class FQL2CollectionDefaultHistoryDaysChangeSpec extends FQL2Spec {
  import Collection.{ MinValidTimeFloorField, RetainDaysField }
  import Document.DefaultRetainDays
  import SchemaNames.{ Name, NameField }

  val NewDefaultRetainDays = 1L
  val collID = UserCollectionID.MinValue
  var auth: Auth = _

  before {
    auth = newDB
  }

  def withNewDefault[A](fn: => A): A = {
    val oldDefault = DefaultRetainDays
    System.setProperty(
      "fauna.model.default-history_days",
      NewDefaultRetainDays.toString)
    try {
      fn
    } finally {
      System.setProperty("fauna.model.default-history_days", oldDefault.toString)
    }
  }

  def doc = (
    ctx ! SchemaCollection
      .Collection(auth.scopeID)
      .get(collID)
  ).value

  "FQL2CollectionDefaultHistoryDaysChangeSpec" - {
    "writes the deafult history_days and computes mvt_floor" - {
      "FQL writes the deafult history_days and computes mvt_floor" - {
        "before" in {
          evalOk(auth, "Collection.create({ name: 'Foo' })")
          doc.data(RetainDaysField).value shouldBe DefaultRetainDays
          doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
        }
        "after" in withNewDefault {
          evalOk(auth, "Collection.create({ name: 'Bar' })")
          doc.data(RetainDaysField).value shouldBe NewDefaultRetainDays
          doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
        }
      }
      "FSL writes the deafult history_days and computes mvt_floor" - {
        "before" in {
          updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")
          doc.data(RetainDaysField).value shouldBe DefaultRetainDays
          doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
        }
        "after" in withNewDefault {
          updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")
          doc.data(RetainDaysField).value shouldBe NewDefaultRetainDays
          doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
        }
      }
    }

    "when replacing a collection definition" - {
      "FQL resets history_days to default and updates mvt_floor" - {
        "before" in {
          evalOk(auth, "Collection.create({ name: 'Foo', history_days: 10 })")
          val before = doc.data(MinValidTimeFloorField)

          evalOk(auth, "Foo.definition.replace({ name: 'Foo' })")
          doc.data(RetainDaysField).value shouldBe DefaultRetainDays
          doc.data(MinValidTimeFloorField) should be > before
        }
        "after" in withNewDefault {
          evalOk(auth, "Collection.create({ name: 'Foo', history_days: 10 })")
          val before = doc.data(MinValidTimeFloorField)

          evalOk(auth, "Foo.definition.replace({ name: 'Foo' })")
          doc.data(RetainDaysField).value shouldBe DefaultRetainDays
          doc.data(MinValidTimeFloorField) should be > before
        }
      }
      "FSL resets history_days to default and updates mvt_floor" - {
        "before" in {
          updateSchemaOk(auth, "main.fsl" -> "collection Foo { history_days 10 }")
          val before = doc.data(MinValidTimeFloorField)

          updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")
          doc.data(RetainDaysField).value shouldBe DefaultRetainDays
          doc.data(MinValidTimeFloorField) should be > before
        }
        "after" in withNewDefault {
          updateSchemaOk(auth, "main.fsl" -> "collection Foo { history_days 10 }")
          val before = doc.data(MinValidTimeFloorField)

          updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")
          doc.data(RetainDaysField).value shouldBe NewDefaultRetainDays
          doc.data(MinValidTimeFloorField) should be > before
        }
        "transition" in {
          updateSchemaOk(auth, "main.fsl" -> "collection Foo {}")
          withNewDefault {
            validateSchemaOk(auth, "main.fsl" -> "collection Foo {}") shouldBe
              """|* Modifying collection `Foo` at main.fsl:1:1:
                 |  * Configuration:
                 |  ~ change history_days from 0 to 1
                 |
                 |""".stripMargin
          }
        }
      }
    }

    "when updating a collection definition via FQL" - {
      "if history_days=SET mvt_floor=SET" - {
        "preserves history_days and updates mvt_floor" - {
          "before" in {
            val floorBefore = Clock.time - 20.days
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  RetainDaysField -> Some(10L),
                  MinValidTimeFloorField -> floorBefore
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField).value shouldBe 10L
            doc.data(MinValidTimeFloorField) should be > floorBefore
          }
          "after" in withNewDefault {
            val floorBefore = Clock.time - 20.days
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  RetainDaysField -> Some(10L),
                  MinValidTimeFloorField -> floorBefore
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField).value shouldBe 10L
            doc.data(MinValidTimeFloorField) should be > floorBefore
          }
        }
      }
      "if history_days=SET mvt_floor=null" - { // legacy data
        "preserves history_days and updates mvt_floor" - {
          "before" in {
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  RetainDaysField -> Some(10L)
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField).value shouldBe 10L
            doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
          }
          "after" in withNewDefault {
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  RetainDaysField -> Some(10L)
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField).value shouldBe 10L
            doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
          }
        }
      }
      "if history_days=null mvt_floor=null" - { // legacy data
        "preserves history_days and updates mvt_floor" - {
          "before" in {
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo")
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField) shouldBe empty
            doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
          }
          "after" in withNewDefault {
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo")
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField) shouldBe empty
            doc.data.getOpt(MinValidTimeFloorField) shouldNot be(empty)
          }
        }
      }
      "if history_days=null mvt_floor=SET" - { // after removing history_days via FQL
        "preserves history_days and updates mvt_floor" - {
          "before" in {
            val floorBefore = Clock.time - 20.days
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  MinValidTimeFloorField -> floorBefore
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField) shouldBe empty
            doc.data(MinValidTimeFloorField) should be > floorBefore
          }
          "after" in withNewDefault {
            val floorBefore = Clock.time - 20.days
            ctx ! SchemaCollection
              .Collection(auth.scopeID)
              .insertCreate(
                collID,
                Clock.time,
                Data(
                  NameField -> Name("Foo"),
                  MinValidTimeFloorField -> floorBefore
                )
              )

            evalOk(auth, "Foo.definition.update({})")
            doc.data(RetainDaysField) shouldBe empty
            doc.data(MinValidTimeFloorField) should be > floorBefore
          }
        }
      }
    }
  }
}
