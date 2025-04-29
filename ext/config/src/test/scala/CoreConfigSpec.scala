package fauna.config.test

import fauna.config._
import java.io.File
import java.nio.file._

class CoreConfigSpec extends Spec {

  var tmpdir: Path = _
  var config: Path = _

  override def beforeAll() = {
    tmpdir = Files.createTempDirectory("coreconfig")
    config = new File(tmpdir.toString + "/faunadb.yml").toPath
    super.beforeAll()
  }

  override def afterAll() = {
    Files.deleteIfExists(config)
    Files.deleteIfExists(tmpdir)
    super.afterAll()
  }

  def writeConfig(pairs: Map[String, Any]) =
    Loader.serialize(config, pairs)

  val defaults = Map("network_broadcast_address" -> "127.0.0.1")

  "CoreConfig" - {
    "accepts a single plaintext root key" in {
      writeConfig(Map("auth_root_key" -> "secret") ++ defaults)
      CoreConfig.load(Some(config)).errors.isEmpty should be(true)
    }

    "accepts a single hashed root key" in {
      writeConfig(Map("auth_root_key_hash" -> "secret") ++ defaults)
      CoreConfig.load(Some(config)).errors.isEmpty should be(true)
    }

    "accepts several hashed root keys" in {
      writeConfig(
        Map("auth_root_key_hashes" -> List("secret1", "secret2")) ++ defaults)
      CoreConfig.load(Some(config)).errors.isEmpty should be(true)
    }
  }
}
