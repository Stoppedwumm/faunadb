package fauna.api.test.queries

import fauna.api.test._
import fauna.codex.json._

class NumericFunctionsSpec extends QueryAPI21Spec {
  "abs" - {
    once("check the abs function") {
      for {
        db <- aDatabase
      } {
        qequals(1, Abs(-1), db)
        qequals(1, Abs(1), db)
        qequals(1.0, Abs(-1.0), db)
        qequals(6, Abs(AddF(-1, -2, -3)), db)
        qequals(10, Abs(-10), db)
        qequals(0, Abs(0), db)
        qassertErr(Abs("5"), "invalid argument", JSArray("abs"), db)
      }
    }
  }

  "add" - {
    once("adds numbers together, or returns itself") {
      for {
        db <- aDatabase
      } {
        qequals(3, AddF(-1, -1, 5), db)
        qequals(4, AddF(2, 2), db)
        qequals(2, AddF(4, -2), db)
        qequals(-2, AddF(-1, -1), db)
        qequals(-1, AddF(-1), db)
        qequals(5, AddF(5), db)
        qassertErr(AddF(), "invalid argument", JSArray("add"), db)
        qassertErr(AddF("5"), "invalid argument", JSArray("add", 0), db)
      }
    }
  }

  "bitand" - {
    once("check the bitand function") {
      for {
        db <- aDatabase
      } {
        qequals(1, BitAnd(1, 1), db)
        qequals(0, BitAnd(1, 0), db)
        qequals(3, BitAnd(3, 7), db)
        qequals(3, BitAnd(7, 3), db)
        qequals(16, BitAnd(16, 17), db)
        qequals(3, BitAnd(15, 7, 3), db)
        qequals(7, BitAnd(255, 7, 15), db)
      }
    }
  }

  "bitnot" - {
    once("check the bitnot function") {
      for {
        db <- aDatabase
      } {
        qequals(0, BitNot(-1), db)
        qequals(-2, BitNot(1), db)
        qequals(-1, BitNot(0), db)
      }
    }
  }

  "bitor" - {
    once("check the bitor function") {
      for {
        db <- aDatabase
      } {
        qequals(15, BitOr(1, 2, 4, 8), db)
        qequals(1, BitOr(1, 0, 1, 1, 0, 1), db)
        qequals(17, BitOr(1, 0, 16), db)
        qequals(0, BitOr(0, 0, 0), db)
      }
    }
  }

  "bitxor" - {
    once("check the bitand function") {
      for {
        db <- aDatabase
      } {
        qequals(1, BitXor(1, 0), db)
        qequals(0, BitXor(1, 0, 1), db)
      }
    }
  }

  "ceil" - {
    once("check the ceil function") {
      for {
        db <- aDatabase
      } {
        qequals(1,   Ceil(1), db)
        qequals(2.0, Ceil(1.1), db)
        qequals(2.0, Ceil(1.5), db)
        qequals(2.0, Ceil(1.9), db)
        qequals(-0.0, Ceil(-0.1), db)
      }
    }
  }

  "divide" - {
    once("computes the quotient, or returns itself") {
      for {
        db <- aDatabase
      } {
        qequals(1, Divide(10, 2, 5), db)
        qequals(0, Divide(2, 5), db)
        qequals(4, Divide(4), db)
        qequals(1, Divide(5, 3), db)
        qequals(5, Divide(5), db)
        qequals(0, Divide(0, 100), db)
        qassertErr(Divide(100, 0), "invalid argument", JSArray(), db)
        qassertErr(Divide(), "invalid argument", JSArray("divide"), db)
      }
    }
  }

  "floor" - {
    once("check the floor function") {
      for {
        db <- aDatabase
      } {
        qequals(1,    Floor(1), db)
        qequals(2.0,  Floor(2.1), db)
        qequals(2.0,  Floor(2.5), db)
        qequals(2.0,  Floor(2.9), db)
        qequals(-1.0, Floor(-0.1), db)
      }
    }
  }

  "modulo" - {
    once("returns the remainer of elements after division") {
      for {
        db <- aDatabase
      } {
        qequals(1, Modulo(100, 11), db)
        qequals(0, Modulo(100, 5), db)
        qequals(1, Modulo(100, 3, 5), db)
        qequals(100, Modulo(100), db)
        qequals(5, Modulo(5), db)
        qassertErr(Modulo(), "invalid argument", JSArray("modulo"), db)
        qassertErr(Modulo("5, 3"), "invalid argument", JSArray("modulo", 0), db)
        qassertErr(Modulo(100, 0), "invalid argument", JSArray(), db)
      }
    }
  }

  "multiply" - {
    once("multiplies numbers, or returns itself") {
      for {
        db <- aDatabase
      } {
        qequals(4, Multiply(2, 2), db)
        qequals(-4, Multiply(-2, 2), db)
        qequals(4, Multiply(-2, -2, 1), db)
        qequals(-2, Multiply(-2), db)
        qequals(15, Multiply(5, 3), db)
        qequals(5, Multiply(5), db)
        qassertErr(Multiply(), "invalid argument", JSArray("multiply"), db)
        qassertErr(Multiply("5"), "invalid argument", JSArray("multiply", 0), db)
      }
    }
  }

  "round" - {
    once("check the round function") {
      for {
        db <- aDatabase
      } {
        qequals(5,      Round(5), db)
        qequals(5.12,   Round(5.123), db)
        qequals(5.67,   Round(5.666), db)
        qequals(6.00,   Round(5.999), db)
        qequals(600.0,  Round(555.666, JSLong(-2)), db)
        qequals(560.0,  Round(555.666, JSLong(-1)), db)
        qequals(556.0,  Round(555.666, JSLong(0)), db)
        qequals(555.7,  Round(555.666, JSLong(1)), db)
        qequals(555.67, Round(555.666, JSLong(2)), db)

        qequals(400.0,  Round(444.444, JSLong(-2)), db)
        qequals(440.0,  Round(444.444, JSLong(-1)), db)
        qequals(444.0,  Round(444.444, JSLong(0)), db)
        qequals(444.4,  Round(444.444, JSLong(1)), db)
        qequals(444.44, Round(444.444, JSLong(2)), db)

        qequals(600.0,  Round(555.666, JSLong(-2)), db)
        qequals(560.0,  Round(555.666, JSLong(-1)), db)
        qequals(400.0,  Round(444.444, JSLong(-2)), db)
        qequals(440.0,  Round(444.444, JSLong(-1)), db)

        qequals(600,    Round(555, JSLong(-2)), db)
        qequals(560,    Round(555, JSLong(-1)), db)
        qequals(400,    Round(444, JSLong(-2)), db)
        qequals(440,    Round(444, JSLong(-1)), db)
      }
    }
  }

  "sign" - {
    once("check the sign function") {
      for {
        db <- aDatabase
      } {
        qequals(1,  Sign(5), db)
        qequals(1,  Sign(5.9), db)
        qequals(-1, Sign(-5.0), db)
        qequals(-1, Sign(-5), db)
      }
    }
  }

  "sqrt" - {
    once("check the sqrt function") {
      for {
        db <- aDatabase
      } {
        qequals(GreaterThan(Sqrt(9), Sqrt(4), Sqrt(1), Sqrt(0)), JSTrue, db)
        qequals(3.0, Sqrt(9), db)
        qequals(2.0, Sqrt(4), db)
      }
    }
  }

  "subtract" - {
    once("subtracts numbers, or returns itself") {
      for {
        db <- aDatabase
      } {
        qequals(2, Subtract(5, 3), db)
        qequals(-2, Subtract(3, 5), db)
        qequals(-4, Subtract(-2, 2, -0), db)
        qequals(-4, Subtract(-4), db)
        qequals(5, Subtract(5), db)
        qassertErr(Subtract(), "invalid argument", JSArray("subtract"), db)
        qassertErr(Subtract("5"), "invalid argument", JSArray("subtract", 0), db)
      }
    }
  }

  "trunc" - {
    once("check the trunc function") {
      for {
        db <- aDatabase
      } {
        qequals(5,       Trunc(5), db)
        qequals(1234.56, Trunc(1234.5678, JSLong(2)), db)
        qequals(1234.5,  Trunc(1234.5678, JSLong(1)), db)
        qequals(1230.0,  Trunc(1234.5678, JSLong(-1)), db)
        qequals(1200.0,  Trunc(1234.5678, JSLong(-2)), db)
        qequals(1200,    Trunc(1234, JSLong(-2)), db)
        qequals(1234,    Trunc(1234, JSLong(2)), db)
      }
    }
  }

}
