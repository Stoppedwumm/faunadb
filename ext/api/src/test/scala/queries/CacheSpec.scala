package fauna.api.test.queries

import fauna.api.test._

class CacheSpec extends QueryAPI21Spec {

  "Cache" - {
    prop("Database: Rename schema should update refs names") {
      for {
        db <- aDatabase
        child <- aDatabase(apiVers, db)
        rename <- aUniqueDBName
      } {
        val renameRef = runQuery(DatabaseRef(rename), db.adminKey)

        (runQuery(Paginate(DatabasesRef), db.adminKey) / "data" / 0) shouldBe child.refObj

        runQuery(Update(child.refObj, MkObject("name" -> rename)), db.adminKey)

        beforeTTLCacheExpiration {
          (runQuery(Paginate(DatabasesRef), db.adminKey) / "data" / 0) shouldBe renameRef
        }
      }
    }

    prop("Collection: Rename schema should update refs names") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        rename <- aUniqueDBName
      } {
        val renameRef = runQuery(ClassRef(rename), db.adminKey)

        (runQuery(Paginate(ClassesNativeClassRef), db.adminKey) / "data" / 0) shouldBe col.refObj

        runQuery(Update(col.refObj, MkObject("name" -> rename)), db.adminKey)

        beforeTTLCacheExpiration {
          (runQuery(Paginate(ClassesNativeClassRef), db.adminKey) / "data" / 0) shouldBe renameRef
        }
      }
    }

    prop("Index: Rename schema should update refs names") {
      for {
        db <- aDatabase
        col <- aCollection(db)
        idx <- anIndex(col)
        rename <- aUniqueDBName
      } {
        val renameRef = runQuery(IndexRef(rename), db.adminKey)

        (runQuery(Paginate(IndexesNativeClassRef), db.adminKey) / "data" / 0) shouldBe idx.refObj

        runQuery(Update(idx.refObj, MkObject("name" -> rename)), db.adminKey)

        beforeTTLCacheExpiration {
          (runQuery(Paginate(IndexesNativeClassRef), db.adminKey) / "data" / 0) shouldBe renameRef
        }
      }
    }

    prop("User Function: Rename schema should update refs names") {
      for {
        db <- aDatabase
        original <- aUniqueDBName
        rename <- aUniqueDBName
      } {
        val func = runQuery(CreateFunction(MkObject(
          "name" -> original,
          "body" -> QueryF(Lambda("i" -> Var("i")))
        )), db.adminKey)

        val renameRef = runQuery(FunctionRef(rename), db.adminKey)

        (runQuery(Paginate(FunctionsNativeClassRef), db.adminKey) / "data" / 0) shouldBe func.refObj

        runQuery(Update(func.refObj, MkObject("name" -> rename)), db.adminKey)

        beforeTTLCacheExpiration {
          (runQuery(Paginate(FunctionsNativeClassRef), db.adminKey) / "data" / 0) shouldBe renameRef
        }
      }
    }

    prop("Role: Rename schema should update refs names") {
      for {
        db <- aDatabase
        role <- aRole(db)
        rename <- aUniqueDBName
      } {
        val renameRef = runQuery(RoleRef(rename), db.adminKey)

        (runQuery(Paginate(RolesNativeClassRef), db.adminKey) / "data" / 0) shouldBe role.refObj

        runQuery(Update(role.refObj, MkObject("name" -> rename)), db.adminKey)

        beforeTTLCacheExpiration {
          (runQuery(Paginate(RolesNativeClassRef), db.adminKey) / "data" / 0) shouldBe renameRef
        }
      }
    }
  }
}
