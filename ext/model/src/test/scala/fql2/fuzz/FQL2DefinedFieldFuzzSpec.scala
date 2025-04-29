package fauna.model.test

import fql.ast.display._
import org.scalacheck.Prop
import scala.util.control.NonFatal

object FQL2PersistedFieldFuzzSpec extends FQL2SchemaFuzzSpec("definedfield") {

  val auth = newDB

  property("create") = Prop.forAll(generateCollection()) { schema =>
    try {
      updateOk(auth, "main.fsl" -> "")
      updateOk(auth, "main.fsl" -> schema.display)
      validateModelCollection(auth, schema)
      PropTestResult.ValidTestPassed
    } catch {
      // NOTE: Stack overflows won't be caught here, so those will skip
      // simplification.
      case NonFatal(_) => PropTestResult.ValidTestFailed
    }
  }

  // FIXME: This test fails in PR, but passes locally. Not sure whats going on
  // with it.
  // property("update") = Prop.forAll(generateCollection(), generateCollection()) {
  //   (schema1, schema2Initial) =>
  //     // This should act like updating a single collection.
  //     val schema2 = schema2Initial.copy(name = schema1.name)
  //
  //     updateOk(auth, "main.fsl" -> "")
  //     updateOk(auth, "main.fsl" -> schema1.display)
  //     validateModelCollection(schema1)
  //     updateOk(auth, "main.fsl" -> schema2.display)
  //     validateModelCollection(schema2)
  //     PropTestResult.ValidTestPassed
  // }
}
