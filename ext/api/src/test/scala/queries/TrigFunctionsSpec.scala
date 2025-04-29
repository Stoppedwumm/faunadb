package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class TrigFunctionsSpec extends QueryAPI21Spec {

  "acos" - {
    once("check the acos function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(ACos(0), ACos(0.1), ACos(0.2)), JSTrue, db)
        qequals(GreaterThan(math.Pi, ACos(0.5)), JSTrue, db)
      }
    }
  }

  "asin" - {
    once("check the asin function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(ASin(0.2), ASin(0.1), ASin(0.0)), JSTrue, db)
        qequals(GreaterThan(ASin(0.5), ASin(0), ASin(-0.5)), JSTrue, db)
      }
    }
  }

  "atan" - {
    once("check the atan function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(ATan(2), ATan(0.2), ATan(0.1), ATan(0.0), ATan(-10)),
                JSTrue,
                db)
        qequals(GreaterThan(ATan(0.5), ATan(0), ATan(-0.5)), JSTrue, db)
      }
    }
  }

  "cos" - {
    once("check the cos function") {
      for {
        db <- aDatabase
      } {
        qequals(1.0, Cos(0), db)
        qequals(GreaterThan(Cos(0), Cos(0.2), Cos(0.5), Cos(1)), JSTrue, db)
      }
    }
  }

  "cosh" - {
    once("check the cosh function") {
      for {
        db <- aDatabase
      } {
        qequals(1.0, Cosh(0), db)
        qequals(3.76, Trunc(Cosh(2),2), db)
      }
    }
  }

  "degrees" - {
    once("check the degrees function") {
      for {
        db <- aDatabase
      } {
        qequals(0.0, Degrees(0), db)
        qequals(114.59, Trunc(Degrees(2),2), db)
      }
    }
  }

  "exp" - {
    once("check the exp function") {
      for {
        db <- aDatabase
      } {
        qequals(1.0, Exp(0), db)
        qequals(GreaterThan(Exp(0.5), Exp(0.1), Exp(0.0), Exp(-0.5)), JSTrue, db)
      }
    }
  }

  "hypot" - {
    once("check the hypot function") {
      for {
        db <- aDatabase
      } {
        qequals(0.0,  Hypot(0), db)
        qequals(0.0,  Hypot(0, 0), db)
        qequals(2.82, Trunc(Hypot(2), 2), db)
        qequals(5.0,  Hypot(3, 4), db)
        qequals(4.24, Trunc(Hypot(3, 3),2), db)
        qequals(6.54, Trunc(Hypot(4.1, 5.1),2), db)
      }
    }
  }

  "ln" - {
    once("check the ln function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(Ln(100), Ln(0.5), Ln(0.1)), JSTrue, db)
      }
    }
  }

  "log" - {
    once("check the Log function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(Log(2), Log(0.5), Log(0.2)), JSTrue, db)
      }
    }
  }

  "pow" - {
    once("check the pow function") {
      for {
        db <- aDatabase
      } {
        qequals(4.0, Pow(2), db)
        qequals(4.0, Pow(-2), db)
        qequals(8.0, Pow(2, JSLong(3)), db)
        qequals(10000.0, Pow(10, JSLong(4)), db)
        qequals(0.001, Pow(10, JSLong(-3)), db)
      }
    }
  }

  "radians" - {
    once("check the radians function") {
      for {
        db <- aDatabase
      } {
        qequals(0.0,  Trunc(Radians(0),2), db)
        qequals(0.87, Trunc(Radians(50),2), db)
      }
    }
  }


  "sin" - {
    once("check the sin function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(Sin(1.5), Sin(1), Sin(0.2)), JSTrue, db)
      }
    }
  }

  "sinh" - {
    once("check the sinh function") {
      for {
        db <- aDatabase
      } {
        qequals(0.0, Trunc(Sinh(0),2), db)
        qequals(3.62, Trunc(Sinh(2),2), db)
      }
    }
  }

  "tan" - {
    once("check the tan function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(Tan(1.5), Tan(1), Tan(0.2)), JSTrue, db)
      }
    }
  }

  "tanh" - {
    once("check the tanh function") {
      for {
        db <- aDatabase
      } {
        qequals(0.0, Trunc(Tanh(0),2), db)
        qequals(1.0, Trunc(Tanh(100),2), db)
      }
    }
  }

}
