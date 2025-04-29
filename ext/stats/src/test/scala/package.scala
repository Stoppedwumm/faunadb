package fauna.stats

import fauna.prop.test.PropSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

package test {

  abstract class Spec
      extends PropSpec(default = 100, exhaustive = 1000)
      with Matchers
      with OptionValues
}
