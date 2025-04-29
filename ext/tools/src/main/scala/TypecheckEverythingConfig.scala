package fauna.tools

import fauna.config.{ Config, Configuration, Loader }
import java.nio.file._

object TypecheckEverythingConfig {

  private val loader =
    new Loader[TypecheckEverythingConfig](new TypecheckEverythingConfig)

  def load(path: Path) = loader.load(path)
}

class TypecheckEverythingConfig extends Configuration {
  @Config var invalid_scopes: List[Long] = Nil
}
