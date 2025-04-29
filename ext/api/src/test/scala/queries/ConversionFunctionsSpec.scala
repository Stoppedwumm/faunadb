package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._
import fauna.lang.syntax._
import fauna.prop.Prop
import java.text.SimpleDateFormat

class ConversionFunctionsSpec extends QueryAPI21Spec {
  "to_string" - {
    prop("converts numbers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToString(int), int.toString, db)
        qequals(ToString(dbl), dbl.toString, db)
      }
    }

    once("converts booleans") {
      for {
        db <- aDatabase
      } {
        qequals(ToString(true), "true", db)
        qequals(ToString(false), "false", db)
      }
    }

    once("converts null") {
      for {
        db <- aDatabase
      } {
        qequals(ToString(JSNull), "null", db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        qequals(ToString(str), str, db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToString(Bytes(bytes)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("converts times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qequals(ToString(Time(time)), time, db)
      }
    }

    prop("converts dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)
        qequals(ToString(Date(str)), str, db)
      }
    }

    prop("converts UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qequals(ToString(UUID(id.toString)), id.toString, db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToString(db.refObj), "invalid argument", JSArray.empty, db)
        qassertErr(ToString(Match(idx.refObj)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToString(JSArray(1, 2, 3)), "invalid argument", JSArray.empty, db)
        qassertErr(ToString(Paginate(set)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToString(Get(db.refObj)), "invalid argument", JSArray.empty, rootKey)
      }
    }

    prop("convert actions") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        inst <-  aDocument(cls)
      } {
        val action = Select(JSArray("data", 0, "action"), Paginate(Events(inst.refObj)))
        qequals(ToString(action), JSString("create"), db)
      }
    }
  }

  "to_number" - {
    prop("converts numbers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToNumber(int), int, db)
        qequals(ToNumber(dbl), dbl, db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToNumber(int.toString), int, db)
        qequals(ToNumber(s" $int "), int, db)
        qequals(ToNumber(dbl.toString), dbl, db)
        qequals(ToNumber(s" $dbl "), dbl, db)
      }
    }

    once("fails to convert booleans") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToNumber(true), "invalid argument", JSArray.empty, db)
        qassertErr(ToNumber(false), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert null") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToNumber(JSNull), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToNumber(Bytes(bytes)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qassertErr(ToNumber(Time(time)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)
        qassertErr(ToNumber(Date(str)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qassertErr(ToNumber(UUID(id.toString)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToNumber(db.refObj), "invalid argument", JSArray.empty, db)
        qassertErr(ToNumber(Match(idx.refObj)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToNumber(JSArray(1, 2, 3)), "invalid argument", JSArray.empty, db)
        qassertErr(ToNumber(Paginate(set)), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToNumber(Get(db.refObj)), "invalid argument", JSArray.empty, rootKey)
      }
    }
  }

  "to_double" - {
    prop("converts numbers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToDouble(int), int.toDouble, db)
        qequals(ToDouble(dbl), dbl, db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToDouble(int.toString), int.toDouble, db)
        qequals(ToDouble(s" $int "), int.toDouble, db)
        qequals(ToDouble(dbl.toString), dbl, db)
        qequals(ToDouble(s" $dbl "), dbl, db)
      }
    }

    once("fails to convert booleans") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDouble(true), "invalid argument", "Cannot cast Boolean to Double.", JSArray("to_double"), db)
        qassertErr(ToDouble(false), "invalid argument", "Cannot cast Boolean to Double.", JSArray("to_double"), db)
      }
    }

    once("fails to convert null") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDouble(JSNull), "invalid argument", "Cannot cast Null to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToDouble(Bytes(bytes)), "invalid argument", "Cannot cast Bytes to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qassertErr(ToDouble(Time(time)), "invalid argument", "Cannot cast Time to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)
        qassertErr(ToDouble(Date(str)), "invalid argument", "Cannot cast Date to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qassertErr(ToDouble(UUID(id.toString)), "invalid argument", "Cannot cast UUID to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToDouble(db.refObj), "invalid argument", "Cannot cast Database Ref to Double.", JSArray("to_double"), db)
        qassertErr(ToDouble(Match(idx.refObj)), "invalid argument", "Cannot cast Set to Double.", JSArray("to_double"), db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToDouble(JSArray(1, 2, 3)), "invalid argument", "Cannot cast Array to Double.", JSArray("to_double"), db)
        qassertErr(ToDouble(set), "invalid argument", "Cannot cast Set to Double.", JSArray("to_double"), db)
        qassertErr(ToDouble(Paginate(set)), "invalid argument", "Cannot cast Page to Double.", JSArray("to_double"), db)
      }
    }

    once("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDouble(Get(db.refObj)), "invalid argument", "Cannot cast Object to Double.", JSArray("to_double"), rootKey)
      }
    }
  }

  "to_integer" - {
    prop("converts numbers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToInteger(int), int, db)
        qequals(ToInteger(dbl), dbl.toInt, db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToInteger(int.toString), int, db)
        qequals(ToInteger(s" $int "), int, db)
        qequals(ToInteger(dbl.toString), dbl.toInt, db)
        qequals(ToInteger(s" $dbl "), dbl.toInt, db)
      }
    }

    once("fails to convert booleans") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToInteger(true), "invalid argument", "Cannot cast Boolean to Integer.", JSArray("to_integer"), db)
        qassertErr(ToInteger(false), "invalid argument", "Cannot cast Boolean to Integer.", JSArray("to_integer"), db)
      }
    }

    once("fails to convert null") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToInteger(JSNull), "invalid argument", "Cannot cast Null to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToInteger(Bytes(bytes)), "invalid argument", "Cannot cast Bytes to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qassertErr(ToInteger(Time(time)), "invalid argument", "Cannot cast Time to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)
        qassertErr(ToInteger(Date(str)), "invalid argument", "Cannot cast Date to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qassertErr(ToInteger(UUID(id.toString)), "invalid argument", "Cannot cast UUID to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToInteger(db.refObj), "invalid argument", "Cannot cast Database Ref to Integer.", JSArray("to_integer"), db)
        qassertErr(ToInteger(Match(idx.refObj)), "invalid argument", "Cannot cast Set to Integer.", JSArray("to_integer"), db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToInteger(JSArray(1, 2, 3)), "invalid argument", "Cannot cast Array to Integer.", JSArray("to_integer"), db)
        qassertErr(ToInteger(set), "invalid argument", "Cannot cast Set to Integer.", JSArray("to_integer"), db)
        qassertErr(ToInteger(Paginate(set)), "invalid argument", "Cannot cast Page to Integer.", JSArray("to_integer"), db)
      }
    }

    once("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToInteger(Get(db.refObj)), "invalid argument", "Cannot cast Object to Integer.", JSArray("to_integer"), rootKey)
      }
    }
  }

  "to_time" - {
    prop("converts integers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qequals(ToTime(int), Epoch(int, "millisecond"), db)
        qassertErr(ToTime(dbl), "invalid argument", JSArray.empty, db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qequals(ToTime(time), Time(time), db)
        qequals(ToTime(s" $time "), Time(time), db)
      }
    }

    once("fails to convert booleans") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToTime(true), "invalid argument", JSArray.empty, db)
        qassertErr(ToTime(false), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert null") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToTime(JSNull), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToTime(Bytes(bytes)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("converts times") {
      for {
        db <- aDatabase
        time <- Prop.isoDate
      } {
        qequals(ToTime(Time(time)), Time(time), db)
      }
    }

    prop("converts dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val dfmt = new SimpleDateFormat("yyyy-MM-dd")
        val dstr = dfmt.format(date)

        val tfmt = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00'Z'")
        val tstr = tfmt.format(date)

        qequals(ToTime(Date(dstr)), Time(tstr), db)
      }
    }

    prop("fails to convert UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qassertErr(ToTime(UUID(id.toString)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToTime(db.refObj), "invalid argument", JSArray.empty, db)
        qassertErr(ToTime(Match(idx.refObj)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToTime(JSArray(1, 2, 3)), "invalid argument", JSArray.empty, db)
        qassertErr(ToTime(Paginate(set)), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToTime(Get(db.refObj)), "invalid argument", JSArray.empty, rootKey)
      }
    }
  }

  "to_date" - {
    prop("fails to convert integers") {
      for {
        db <- aDatabase
        int <- Prop.int
        dbl <- Prop.double
      } {
        qassertErr(ToDate(int), "invalid argument", JSArray.empty, db)
        qassertErr(ToDate(dbl), "invalid argument", JSArray.empty, db)
      }
    }

    prop("converts strings") {
      for {
        db <- aDatabase
        date <- Prop.date
        junk <- Prop.string
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)

        qequals(ToDate(str), Date(str), db)
        qequals(ToDate(s" $str "), Date(str), db)
        qassertErr(ToDate(junk), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert booleans") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDate(true), "invalid argument", JSArray.empty, db)
        qassertErr(ToDate(false), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert null") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDate(JSNull), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert bytes") {
      for {
        db <- aDatabase
        str <- Prop.string
      } {
        val bytes = str.toUTF8Bytes
        qassertErr(ToDate(Bytes(bytes)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("converts times") {
      for {
        db <- aDatabase
        time <- Prop.date
      } {
        val dfmt = new SimpleDateFormat("yyyy-MM-dd")
        val dstr = dfmt.format(time)

        val tfmt = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00'Z'")
        val tstr = tfmt.format(time)

        qequals(ToDate(Time(tstr)), Date(dstr), db)
      }
    }

    prop("converts dates") {
      for {
        db <- aDatabase
        date <- Prop.date
      } {
        val fmt = new SimpleDateFormat("yyyy-MM-dd")
        val str = fmt.format(date)
        qequals(ToDate(Date(str)), Date(str), db)
      }
    }

    prop("fails to convert UUIDs") {
      for {
        db <- aDatabase
        id <- Prop.uuid
      } {
        qassertErr(ToDate(UUID(id.toString)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert references") {
      for {
        db <- aDatabase
        cls <- aCollection(db)
        idx <- anIndex(cls)
      } {
        qassertErr(ToDate(db.refObj), "invalid argument", JSArray.empty, db)
        qassertErr(ToDate(Match(idx.refObj)), "invalid argument", JSArray.empty, db)
      }
    }

    prop("fails to convert collections") {
      for {
        db <- aDatabase
        (_, _, _, set) <- aMatchSet(db)
      } {
        qassertErr(ToDate(JSArray(1, 2, 3)), "invalid argument", JSArray.empty, db)
        qassertErr(ToDate(Paginate(set)), "invalid argument", JSArray.empty, db)
      }
    }

    once("fails to convert instances") {
      for {
        db <- aDatabase
      } {
        qassertErr(ToDate(Get(db.refObj)), "invalid argument", JSArray.empty, rootKey)
      }
    }
  }

  "to_object" - {
    once("convert array of (field, values)") {
      for {
        db <- aDatabase
      } {
        qequals(
          ToObject(JSArray.empty),
          MkObject(),
          db
        )

        qequals(
          ToObject(JSArray(JSArray("x", 10), JSArray("y", 20))),
          MkObject("x" -> 10, "y" -> 20),
          db
        )
      }
    }

    once("convert objects") {
      for {
        db <- aDatabase
      } {
        qequals(
          ToObject(MkObject()),
          MkObject(),
          db
        )

        qequals(
          ToObject(MkObject("x" -> 10, "y" -> 20)),
          MkObject("x" -> 10, "y" -> 20),
          db
        )
      }
    }

    once("preserve null values") {
      for {
        db <- aDatabase
      } {
        qequals(
          ToObject(JSArray(JSArray("x", JSNull), JSArray("y", JSNull))),
          MkObject("x" -> JSNull, "y" -> JSNull),
          db
        )
      }
    }

    once("convert version") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("x" -> 10, "y" -> 20)))
      } {
        runQuery(
          ToObject(Get(doc.refObj)),
          db
        ) shouldBe JSObject("ref" -> doc.refObj, "ts" -> doc.ts, "data" -> JSObject("x" -> 10, "y" -> 20))
      }
    }

    once("dynamic keys") {
      for {
        db <- aDatabase
      } {
        qequals(
          ToObject(MapF(Lambda("i" -> JSArray(ToString(Var("i")), Var("i"))), JSArray(1, 2, 3))),
          MkObject("1" -> 1, "2" -> 2, "3" -> 3),
          db
        )
      }
    }

    once("convert pages") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, Prop.const(Seq.empty), Prop.const(Seq((JSArray("data", "name"), false), (JSArray("data", "value"), false))))
        _ <- aDocument(coll, dataProp = Prop.const(JSObject("name" -> "x", "value" -> 10)))
        _ <- aDocument(coll, dataProp = Prop.const(JSObject("name" -> "y", "value" -> 20)))
      } {
        runQuery(ToObject(Select("data", Paginate(Match(idx.refObj)))), db) shouldBe JSObject("x" -> 10, "y" -> 20)
      }
    }

    once("create/update document") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
      } {
        val doc0 = runQuery(CreateF(coll.refObj, MkObject("data" -> ToObject(JSArray(JSArray("x", 10), JSArray("y", 20), JSArray("z", JSNull))))), db)
        (doc0 / "data") shouldBe JSObject("x" -> 10, "y" -> 20)

        val doc1 = runQuery(Update(doc0.refObj, MkObject("data" -> ToObject(JSArray(JSArray("x", JSNull), JSArray("y", 20), JSArray("z", 30))))), db)
        (doc1 / "data") shouldBe JSObject("y" -> 20, "z" -> 30)
      }
    }

    once("invalid cast") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        qassertErr(ToObject("str"), "invalid argument", "Cannot cast String to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(10L), "invalid argument", "Cannot cast Integer to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(10D), "invalid argument", "Cannot cast Double to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(Now()), "invalid argument", "Cannot cast Time to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(Date("1970-01-01")), "invalid argument", "Cannot cast Date to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(IndexesRef), "invalid argument", "Cannot cast Collection Ref to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(Match(idx.refObj)), "invalid argument", "Cannot cast Set to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(Paginate(Match(idx.refObj))), "invalid argument", "Cannot cast Page to Object.", JSArray("to_object"), db)
        qassertErr(ToObject(JSArray(JSArray(10, 20))), "invalid argument", "Field/Value expected, Array provided.", JSArray("to_object", 0), db)
      }
    }
  }

  "to_array" - {
    once("convert objects") {
      for {
        db <- aDatabase
      } {
        qequals(ToArray(MkObject("x" -> 10, "y" -> 20)), JSArray(JSArray("x", 10), JSArray("y", 20)), db)
        qequals(ToArray(MkObject()), JSArray.empty, db)
      }
    }

    once("convert arrays") {
      for {
        db <- aDatabase
      } {
        qequals(ToArray(JSArray(JSArray("x", 10), JSArray("y", 20))), JSArray(JSArray("x", 10), JSArray("y", 20)), db)
        qequals(ToArray(JSArray.empty), JSArray.empty, db)
      }
    }

    once("preserve null values") {
      for {
        db <- aDatabase
      } {
        qequals(ToArray(MkObject("x" -> 10, "y" -> 20, "z" -> JSNull)), JSArray(JSArray("x", 10), JSArray("y", 20), JSArray("z", JSNull)), db)
      }
    }

    once("convert version") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("x" -> 10, "y" -> 20)))
      } {
        runQuery(
          ToArray(Get(doc.refObj)),
          db
        ) shouldBe JSArray(
          JSArray("ref", doc.refObj),
          JSArray("ts", doc.ts),
          JSArray("data", JSObject("x" -> 10, "y" -> 20))
        )
      }
    }

    once("invalid cast") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        qassertErr(ToArray("str"), "invalid argument", "Cannot cast String to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(10L), "invalid argument", "Cannot cast Integer to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(10D), "invalid argument", "Cannot cast Double to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(Now()), "invalid argument", "Cannot cast Time to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(Date("1970-01-01")), "invalid argument", "Cannot cast Date to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(IndexesRef), "invalid argument", "Cannot cast Collection Ref to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(Match(idx.refObj)), "invalid argument", "Cannot cast Set to Array.", JSArray("to_array"), db)
        qassertErr(ToArray(Paginate(Match(idx.refObj))), "invalid argument", "Cannot cast Page to Array.", JSArray("to_array"), db)
      }
    }
  }
}


class ConversionFunctionsV20Spec extends QueryAPI20Spec {

  "to_object" - {
    once("convert version") {
      for {
        db <- aDatabase
        coll <- aFaunaClass(db)
        doc <- aDocument(coll, dataProp = Prop.const(JSObject("x" -> 10, "y" -> 20)))
      } {
        runQuery(
          ToObject(Get(doc.refObj)),
          db
        ) shouldBe JSObject("ref" -> doc.refObj, "class" -> coll.refObj, "ts" -> doc.ts, "data" -> JSObject("x" -> 10, "y" -> 20))
      }
    }
  }
}
