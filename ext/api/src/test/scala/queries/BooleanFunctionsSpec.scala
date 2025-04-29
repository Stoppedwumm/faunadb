package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class BooleanFunctionsSpec extends QueryAPI27Spec {
  "and" - {
    once("computes the conjunction.") {
      for {
        db <- aDatabase
      } {
        qequals(And(JSTrue), JSTrue, db)
        qequals(And(JSFalse, JSTrue), JSFalse, db)
      }
    }

    once("is not short-circuited") {
      for {
        db <- aDatabase
        collName <- aName
      } {
        val andExpr = And(
          JSFalse,
          Do(CreateCollection(MkObject("name" -> collName)), JSTrue)
        )

        qequals(andExpr, JSFalse, db)
        qassert(Exists(ClassRef(collName)), db)
      }
    }
  }

  "or" - {
    once("computes the disjunction.") {
      for {
        db <- aDatabase
      } {
        qequals(Or(JSFalse), JSFalse, db)
        qequals(Or(JSFalse, JSTrue), JSTrue, db)
      }
    }

    once("is not short-circuited") {
      for {
        db <- aDatabase
        collName <- aName
      } {
        val orExpr = Or(
          JSTrue,
          Do(CreateCollection(MkObject("name" -> collName)), JSFalse)
        )

        qequals(orExpr, JSTrue, db)
        qassert(Exists(ClassRef(collName)), db)
      }
    }
  }

  "not" - {
    once("computes the negation.") {
      for {
        db <- aDatabase
      } {
        qequals(Not(JSFalse), JSTrue, db)
        qequals(Not(JSTrue), JSFalse, db)
      }
    }
  }
}

class BooleanFunctionsV3Spec extends QueryAPI3Spec {
  "and" - {
    once("is short-circuited") {
      for {
        db <- aDatabase
        collName <- aName
      } {
        val andExpr = And(
          JSFalse,
          Do(CreateCollection(MkObject("name" -> collName)), JSTrue)
        )

        qequals(andExpr, JSFalse, db)
        qassert(Not(Exists(ClassRef(collName))), db)
      }
    }

    once("should accept single arguments") {
      for {
        db <- aDatabase
      } {
        qequals(JSObject("and" -> JSTrue), JSTrue, db)
        qequals(JSObject("and" -> JSFalse), JSFalse, db)
      }
    }
  }

  "or" - {
    once("is short-circuited") {
      for {
        db <- aDatabase
        collName <- aName
      } {
        val orExpr = Or(
          JSTrue,
          Do(CreateCollection(MkObject("name" -> collName)), JSFalse)
        )

        qequals(orExpr, JSTrue, db)
        qassert(Not(Exists(ClassRef(collName))), db)
      }
    }

    once("should accept single arguments") {
      for {
        db <- aDatabase
      } {
        qequals(JSObject("or" -> JSTrue), JSTrue, db)
        qequals(JSObject("or" -> JSFalse), JSFalse, db)
      }
    }
  }
}
