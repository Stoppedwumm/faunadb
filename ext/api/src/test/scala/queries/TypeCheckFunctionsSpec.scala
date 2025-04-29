package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json.{ JSArray, JSNull }
import fauna.prop.Prop
import scala.util.Random

class TypeCheckFunctionsSpec extends QueryAPI21Spec {

  "is_number" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsNumber(10L), db)
        qassert(IsNumber(3.14), db)

        qassert(Not(IsNumber("a string")), db)
        qassert(Not(IsNumber(JSArray.empty)), db)
        qassert(Not(IsNumber(MkObject())), db)
        qassert(Not(IsNumber(db.refObj)), db)
        qassert(Not(IsNumber(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_double" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsDouble(3.14), db)

        qassert(Not(IsDouble(10L)), db)
        qassert(Not(IsDouble("a string")), db)
        qassert(Not(IsDouble(JSArray.empty)), db)
        qassert(Not(IsDouble(MkObject())), db)
        qassert(Not(IsDouble(db.refObj)), db)
        qassert(Not(IsDouble(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_integer" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsInteger(10L), db)

        qassert(Not(IsInteger(3.14)), db)
        qassert(Not(IsInteger("a string")), db)
        qassert(Not(IsInteger(JSArray.empty)), db)
        qassert(Not(IsInteger(MkObject())), db)
        qassert(Not(IsInteger(db.refObj)), db)
        qassert(Not(IsInteger(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_boolean" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsBoolean(true), db)
        qassert(IsBoolean(false), db)

        qassert(Not(IsBoolean(3.14)), db)
        qassert(Not(IsBoolean(10L)), db)
        qassert(Not(IsBoolean("a string")), db)
        qassert(Not(IsBoolean(JSArray.empty)), db)
        qassert(Not(IsBoolean(MkObject())), db)
        qassert(Not(IsBoolean(db.refObj)), db)
        qassert(Not(IsBoolean(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_null" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsNull(JSNull), db)

        qassert(Not(IsNull(3.14)), db)
        qassert(Not(IsNull(10L)), db)
        qassert(Not(IsNull("a string")), db)
        qassert(Not(IsNull(JSArray.empty)), db)
        qassert(Not(IsNull(MkObject())), db)
        qassert(Not(IsNull(db.refObj)), db)
        qassert(Not(IsNull(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_bytes" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        val bytes = Bytes(Array[Byte](1, 2, 3, 4))

        qassert(IsBytes(bytes), db)

        qassert(Not(IsBytes(3.14)), db)
        qassert(Not(IsBytes(10L)), db)
        qassert(Not(IsBytes("a string")), db)
        qassert(Not(IsBytes(JSArray.empty)), db)
        qassert(Not(IsBytes(MkObject())), db)
        qassert(Not(IsBytes(db.refObj)), db)
        qassert(Not(IsBytes(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_timestamp" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsTimestamp(Now()), db)
        qassert(IsTimestamp(Time("1970-01-01T00:00:00Z")), db)
        qassert(IsTimestamp(Epoch(1, "milliseconds")), db)

        qassert(Not(IsTimestamp(DateF("1970-01-01"))), db)
        qassert(Not(IsTimestamp(3.14)), db)
        qassert(Not(IsTimestamp(10L)), db)
        qassert(Not(IsTimestamp("a string")), db)
        qassert(Not(IsTimestamp(JSArray.empty)), db)
        qassert(Not(IsTimestamp(MkObject())), db)
        qassert(Not(IsTimestamp(db.refObj)), db)
        qassert(Not(IsTimestamp(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_date" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsDate(DateF("1970-01-01")), db)
        qassert(IsDate(ToDate(Now())), db)

        qassert(Not(IsDate(Now())), db)
        qassert(Not(IsDate(3.14)), db)
        qassert(Not(IsDate(10L)), db)
        qassert(Not(IsDate("a string")), db)
        qassert(Not(IsDate(JSArray.empty)), db)
        qassert(Not(IsDate(MkObject())), db)
        qassert(Not(IsDate(db.refObj)), db)
        qassert(Not(IsDate(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_uuid" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsUUID(UUID(java.util.UUID.randomUUID().toString())), db)

        qassert(Not(IsUUID(Now())), db)
        qassert(Not(IsUUID(3.14)), db)
        qassert(Not(IsUUID(10L)), db)
        qassert(Not(IsUUID("a string")), db)
        qassert(Not(IsUUID(JSArray.empty)), db)
        qassert(Not(IsUUID(MkObject())), db)
        qassert(Not(IsUUID(db.refObj)), db)
        qassert(Not(IsUUID(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_string" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        doc <- aDocument(coll)
      } {
        qassert(IsString("a string"), db)

        //action
        val action = Select("action", InsertVers(doc.refObj, Now(), "create", MkObject()))
        qassert(IsString(action), db)

        qassert(IsString(Select("id", DatabaseNativeClassRef)), db)
        qassert(IsString(Select("id", ClassesNativeClassRef)), db)
        qassert(IsString(Select("id", KeysNativeClassRef)), db)
        qassert(IsString(Select("id", TokensNativeClassRef)), db)
        qassert(IsString(Select("id", IndexesNativeClassRef)), db)
        qassert(IsString(Select("id", CredentialsNativeClassRef)), db)
        qassert(IsString(Select("id", FunctionsNativeClassRef)), db)
        qassert(IsString(Select("id", RolesNativeClassRef)), db)

        qassert(Not(IsString(Now())), db)
        qassert(Not(IsString(3.14)), db)
        qassert(Not(IsString(10L)), db)
        qassert(Not(IsString(JSArray.empty)), db)
        qassert(Not(IsString(MkObject())), db)
        qassert(Not(IsString(db.refObj)), db)
        qassert(Not(IsString(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_array" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll, termProp = Prop.const(Seq.empty))
        _ <- aDocument(coll)
        _ <- aDocument(coll)
      } {
        qassert(IsArray(JSArray.empty), db)
        qassert(IsArray(JSArray(1, 2, 3)), db)

        //cursor
        qassert(IsArray(Select("after", Paginate(Match(idx.refObj), size = 1))), db)

        qassert(Not(IsArray(Now())), db)
        qassert(Not(IsArray(3.14)), db)
        qassert(Not(IsArray(10L)), db)
        qassert(Not(IsArray("a string")), db)
        qassert(Not(IsArray(MkObject())), db)
        qassert(Not(IsArray(db.refObj)), db)
        qassert(Not(IsArray(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_object" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        doc <- aDocument(coll)
      } {
        qassert(IsObject(MkObject()), db)
        qassert(IsObject(MkObject("foo" -> "bar")), db)

        //version
        qassert(IsObject(Get(coll.refObj)), db)
        qassert(IsObject(Get(idx.refObj)), db)
        qassert(IsObject(Get(doc.refObj)), db)
        qassert(IsObject(Get(db.refObj)), rootKey)

        //page
        qassert(IsObject(Paginate(Match(idx.refObj))), db)

        //event
        qassert(IsObject(InsertVers(doc.refObj, Now(), "create", MkObject())), db)

        //cursor
        qassert(IsObject(Select("after", Paginate(Events(doc.refObj), size = 1))), db)

        qassert(Not(IsObject(Now())), db)
        qassert(Not(IsObject(3.14)), db)
        qassert(Not(IsObject(10L)), db)
        qassert(Not(IsObject("a string")), db)
        qassert(Not(IsObject(JSArray.empty)), db)
        qassert(Not(IsObject(db.refObj)), db)
      }
    }
  }

  "is_ref" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        doc <- aDocument(coll)
      } {
        qassert(IsRef(doc.refObj), db)
        qassert(IsRef(idx.refObj), db)
        qassert(IsRef(coll.refObj), db)
        qassert(IsRef(db.refObj), rootKey)

        //unresolved ref
        qassert(IsRef(db.refObj), db)
        qassert(IsRef(MkRef(ClassRef("cls"), "1")), db)

        qassert(Not(IsRef(Now())), db)
        qassert(Not(IsRef(3.14)), db)
        qassert(Not(IsRef(10L)), db)
        qassert(Not(IsRef("a string")), db)
        qassert(Not(IsRef(JSArray.empty)), db)
        qassert(Not(IsRef(MkObject())), db)
        qassert(Not(IsRef(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_set" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        doc <- aDocument(coll)
      } {
        //schema sets
        qassert(IsSet(DatabaseNativeClassRef), db)
        qassert(IsSet(ClassesNativeClassRef), db)
        qassert(IsSet(KeysNativeClassRef), db)
        qassert(IsSet(IndexesNativeClassRef), db)
        qassert(IsSet(FunctionsNativeClassRef), db)
        qassert(IsSet(RolesNativeClassRef), db)

        //sets
        qassert(IsSet(Match(idx.refObj)), db)
        qassert(IsSet(Events(doc.refObj)), db)

        qassert(Not(IsSet(Now())), db)
        qassert(Not(IsSet(3.14)), db)
        qassert(Not(IsSet(10L)), db)
        qassert(Not(IsSet("a string")), db)
        qassert(Not(IsSet(JSArray.empty)), db)
        qassert(Not(IsSet(MkObject())), db)
        qassert(Not(IsSet(Get(db.refObj))), rootKey)
      }
    }
  }

  "is_doc" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        doc <- aDocument(coll)
      } {
        qassert(IsDoc(doc.refObj), db)
        qassert(IsDoc(Get(doc.refObj)), db)

        qassert(IsDoc(idx.refObj), db)
        qassert(IsDoc(Get(idx.refObj)), db)

        qassert(IsDoc(coll.refObj), db)
        qassert(IsDoc(Get(coll.refObj)), db)

        qassert(IsDoc(db.refObj), rootKey)
        qassert(IsDoc(Get(db.refObj)), rootKey)

        qassert(Not(IsDoc(Now())), db)
        qassert(Not(IsDoc(3.14)), db)
        qassert(Not(IsDoc(10L)), db)
        qassert(Not(IsDoc("a string")), db)
        qassert(Not(IsDoc(JSArray.empty)), db)
        qassert(Not(IsDoc(MkObject())), db)
      }
    }
  }

  "is_lambda" - {
    once("works") {
      for {
        db <- aDatabase
      } {
        qassert(IsLambda(QueryF(Lambda("x" -> Var("x")))), db)

        qassert(Not(IsLambda(Now())), db)
        qassert(Not(IsLambda(3.14)), db)
        qassert(Not(IsLambda(10L)), db)
        qassert(Not(IsLambda("a string")), db)
        qassert(Not(IsLambda(JSArray.empty)), db)
        qassert(Not(IsLambda(MkObject())), db)
      }
    }
  }

  "is_collection" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        qassert(IsCollection(coll.refObj), db)
        qassert(IsCollection(Get(coll.refObj)), db)

        qassert(Not(IsCollection(db.refObj)), rootKey)
        qassert(Not(IsCollection(Get(db.refObj))), rootKey)

        qassert(Not(IsCollection(idx.refObj)), db)
        qassert(Not(IsCollection(Get(idx.refObj))), db)

        qassert(Not(IsCollection("a string")), db)
        qassert(Not(IsCollection(JSArray.empty)), db)
        qassert(Not(IsCollection(MkObject())), db)
        qassert(Not(IsCollection(3.14)), db)
        qassert(Not(IsCollection(10L)), db)
      }
    }
  }

  "is_database" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        qassert(IsDatabase(db.refObj), rootKey)
        qassert(IsDatabase(Get(db.refObj)), rootKey)

        qassert(Not(IsDatabase(coll.refObj)), db)
        qassert(Not(IsDatabase(Get(coll.refObj))), db)

        qassert(Not(IsDatabase(idx.refObj)), db)
        qassert(Not(IsDatabase(Get(idx.refObj))), db)

        qassert(Not(IsDatabase("a string")), db)
        qassert(Not(IsDatabase(JSArray.empty)), db)
        qassert(Not(IsDatabase(MkObject())), db)
        qassert(Not(IsDatabase(3.14)), db)
        qassert(Not(IsDatabase(10L)), db)
      }
    }
  }

  "is_index" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        qassert(IsIndex(idx.refObj), db)
        qassert(IsIndex(Get(idx.refObj)), db)

        qassert(Not(IsIndex(coll.refObj)), db)
        qassert(Not(IsIndex(Get(coll.refObj))), db)

        qassert(Not(IsIndex(db.refObj)), rootKey)
        qassert(Not(IsIndex(Get(db.refObj))), rootKey)

        qassert(Not(IsIndex("a string")), db)
        qassert(Not(IsIndex(JSArray.empty)), db)
        qassert(Not(IsIndex(MkObject())), db)
        qassert(Not(IsIndex(3.14)), db)
        qassert(Not(IsIndex(10L)), db)
      }
    }
  }

  "is_function" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        val func = runQuery(CreateFunction(MkObject(
          "name" -> Random.alphanumeric.take(20).mkString,
          "body" -> QueryF(Lambda("x" -> Var("x")))
        )), db)

        qassert(IsFunction(func.refObj), db)
        qassert(IsFunction(Get(func.refObj)), db)

        qassert(Not(IsFunction(idx.refObj)), db)
        qassert(Not(IsFunction(Get(idx.refObj))), db)

        qassert(Not(IsFunction(coll.refObj)), db)
        qassert(Not(IsFunction(Get(coll.refObj))), db)

        qassert(Not(IsFunction(db.refObj)), rootKey)
        qassert(Not(IsFunction(Get(db.refObj))), rootKey)

        qassert(Not(IsFunction("a string")), db)
        qassert(Not(IsFunction(JSArray.empty)), db)
        qassert(Not(IsFunction(MkObject())), db)
        qassert(Not(IsFunction(3.14)), db)
        qassert(Not(IsFunction(10L)), db)
      }
    }
  }

  "is_key" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        val key = runQuery(CreateKey(MkObject(
          "database" -> db.refObj,
          "role" -> "server"
        )), rootKey)

        qassert(IsKey(key.refObj), rootKey)
        qassert(IsKey(Get(key.refObj)), rootKey)

        qassert(Not(IsKey(idx.refObj)), db)
        qassert(Not(IsKey(Get(idx.refObj))), db)

        qassert(Not(IsKey(coll.refObj)), db)
        qassert(Not(IsKey(Get(coll.refObj))), db)

        qassert(Not(IsKey(db.refObj)), rootKey)
        qassert(Not(IsKey(Get(db.refObj))), rootKey)

        qassert(Not(IsKey("a string")), db)
        qassert(Not(IsKey(JSArray.empty)), db)
        qassert(Not(IsKey(MkObject())), db)
        qassert(Not(IsKey(3.14)), db)
        qassert(Not(IsKey(10L)), db)
      }
    }
  }

  "is_token" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        val doc = runQuery(CreateF(coll.refObj, MkObject(
          "credentials" -> MkObject("password" -> "sekret")
        )), db)

        val token = runQuery(Login(doc.refObj, MkObject("password" -> "sekret")), db)

        qassert(IsToken(token.refObj), db)
        qassert(IsToken(Get(token.refObj)), db)

        qassert(Not(IsToken(idx.refObj)), db)
        qassert(Not(IsToken(Get(idx.refObj))), db)

        qassert(Not(IsToken(coll.refObj)), db)
        qassert(Not(IsToken(Get(coll.refObj))), db)

        qassert(Not(IsToken(db.refObj)), rootKey)
        qassert(Not(IsToken(Get(db.refObj))), rootKey)

        qassert(Not(IsToken("a string")), db)
        qassert(Not(IsToken(JSArray.empty)), db)
        qassert(Not(IsToken(MkObject())), db)
        qassert(Not(IsToken(3.14)), db)
        qassert(Not(IsToken(10L)), db)
      }
    }
  }

  "is_credentials" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
      } {
        val doc = runQuery(CreateF(coll.refObj, MkObject(
          "credentials" -> MkObject("password" -> "sekret")
        )), db)

        val token = runQuery(Login(doc.refObj, MkObject("password" -> "sekret")), db)

        val cred = runQuery(Get(Ref("credentials/self")), (token / "secret").as[String])

        qassert(IsCredentials(cred.refObj), db)
        qassert(IsCredentials(Get(cred.refObj)), db)

        qassert(Not(IsCredentials(token.refObj)), db)
        qassert(Not(IsCredentials(Get(token.refObj))), db)

        qassert(Not(IsCredentials(idx.refObj)), db)
        qassert(Not(IsCredentials(Get(idx.refObj))), db)

        qassert(Not(IsCredentials(coll.refObj)), db)
        qassert(Not(IsCredentials(Get(coll.refObj))), db)

        qassert(Not(IsCredentials(db.refObj)), rootKey)
        qassert(Not(IsCredentials(Get(db.refObj))), rootKey)

        qassert(Not(IsCredentials("a string")), db)
        qassert(Not(IsCredentials(JSArray.empty)), db)
        qassert(Not(IsCredentials(MkObject())), db)
        qassert(Not(IsCredentials(3.14)), db)
        qassert(Not(IsCredentials(10L)), db)
      }
    }
  }

  "is_role" - {
    once("works") {
      for {
        db <- aDatabase
        coll <- aCollection(db)
        idx <- anIndex(coll)
        role <- aRole(db)
      } {
        qassert(IsRole(role.refObj), db.adminKey)
        qassert(IsRole(Get(role.refObj)), db.adminKey)

        qassert(Not(IsRole(coll.refObj)), db)
        qassert(Not(IsRole(Get(coll.refObj))), db)

        qassert(Not(IsRole(db.refObj)), rootKey)
        qassert(Not(IsRole(Get(db.refObj))), rootKey)

        qassert(Not(IsRole(idx.refObj)), db)
        qassert(Not(IsRole(Get(idx.refObj))), db)

        qassert(Not(IsRole("a string")), db)
        qassert(Not(IsRole(JSArray.empty)), db)
        qassert(Not(IsRole(MkObject())), db)
        qassert(Not(IsRole(3.14)), db)
        qassert(Not(IsRole(10L)), db)
      }
    }
  }
}
