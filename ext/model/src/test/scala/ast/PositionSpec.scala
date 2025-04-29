package fauna.model.test

import fauna.ast._

class PositionSpec extends Spec with ASTHelpers {
  "Position" - {
    val pos = RootPosition.at(1).at("foo").at(2).at("bar")

    "Converts to list of elems" in {
      pos.toElems should equal (List(Left(1), Right("foo"), Left(2), Right("bar")))
    }

    "Converts to String" in {
      pos.toString should equal ("""[1,"foo",2,"bar"]""")
    }
  }
}
