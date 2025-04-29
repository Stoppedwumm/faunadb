package fauna.config.test

import fauna.config._
import java.io.File
import java.nio.file.{ Files, Path }
import scala.collection.immutable.HashMap

// Annoyingly, we basically have to roll the entirety of a case class here manually, but with EXTRA MUTABILITY, because
// for type T in a Loader[T], T must have a single unambiguous constructor that is nullary in its parameters.
class SampleConfig extends Configuration {
  @Config val a: Int = -1
  @Config val b: String = ""
  @Config(Required) val required: String = ""
  @Config(Deprecated) val deprecated: String = ""
  @Config(Renamed("new_field")) val renamed_field: String = ""
  @Config val new_field: String = ""
  @Config val bigger: Long = -1
  @Config val longs: List[Long] = Nil
  @Config val strings: List[String] = Nil

  def privateRenamedField = renamed_field

  def kvs() =
    HashMap[String, Any](
      "a" -> a,
      "b" -> b,
      "required" -> required,
      "new_field" -> new_field
    )

  override def equals(o: Any) = o match {
    case that: SampleConfig =>
      that.a == a &&
      that.b == b &&
      that.required == required &&
      that.renamed_field == renamed_field &&
      that.new_field == new_field
    case _ => false
  }

  override def toString: String =
    s"SampleLoadableType(${a}, ${b}, ${required}, ${renamed_field}, ${new_field})"
}

object SampleConfig {

  def apply(a: Int, b: String, required: String) = {
    val s = new SampleConfig
    s.a = a
    s.b = b
    s.required = required
    s
  }
}

class LoaderSpec extends Spec {
  var tmpdir: Path = null
  var cPath: Path = null

  def writeConfig(kvs: HashMap[String, Any]): Unit = {
    if (cPath == null) {
      throw new RuntimeException("beforeAll should have been run first!")
    }

    Loader.serialize(cPath, kvs)
  }

  override def beforeAll(): Unit = {
    // A YAML file must already be present.
    tmpdir = Files.createTempDirectory("fauna-core-test-configloaderspec")
    cPath = new File(tmpdir.toString + "/config.yml").toPath

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Files.deleteIfExists(cPath)
    Files.deleteIfExists(tmpdir)

    super.afterAll()
  }

  "Loader" - {
    def newLoader = Loader(new SampleConfig)
    val initial = SampleConfig(42, "brownie", "yak")

    "unmarshals from a config file into the right datatype" in {
      writeConfig(initial.kvs())

      val l = newLoader.load(cPath)
      l.warnings.length should equal(0)
      l.errors.length should equal(0)
      l.config should equal(initial)
    }

    "retains default config var values if (non-required) key is absent" in {
      val newReqdFld = "this is fine"
      writeConfig(HashMap[String, Any]("required" -> newReqdFld))
      val l = newLoader.load(cPath)

      l.warnings.length should equal(0)
      l.errors.length should equal(0)

      val nascentConfig = new SampleConfig
      l.config should equal(
        SampleConfig(nascentConfig.a, nascentConfig.b, newReqdFld)
      )
    }

    "complains on a type mismatch" in {
      val kvs = HashMap[String, Any]("required" -> 42) /* should be a string */
      writeConfig(kvs)

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(List(InvalidSetting(
        "required",
        42,
        "42 (class java.lang.Integer) is not an instance of class java.lang.String")))
    }

    "complains about fields that are deprecated" in {
      writeConfig(initial.kvs() + ("deprecated" -> ""))

      val l = newLoader.load(cPath)
      l.warnings should equal(List(DeprecatedSetting("deprecated")))
      l.errors should equal(Nil)
    }

    "requires required fields to exist" in {
      writeConfig(initial.kvs() - "required")

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(List(MissingRequiredSetting("required")))
    }

    "requires required fields to be non-null" in {
      writeConfig(initial.kvs() + ("required" -> null))

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(List(MissingRequiredSetting("required")))
    }

    "handles renamed fields" in {
      writeConfig(initial.kvs() + ("renamed_field" -> "something"))

      val l = newLoader.load(cPath)
      l.config.privateRenamedField should equal("something")
      l.config.new_field should equal("")
      l.warnings should equal(List(RenamedSetting("renamed_field", "new_field")))
      l.errors should equal(Nil)
    }

    "overridden fields will be written" in {
      writeConfig(HashMap[String, Any]("required" -> "satsuma"))

      val l = newLoader.load(cPath, Map("b" -> "rocky"))
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      // Even though the config did not have a "b" field written to it, the
      // overridden field will be written.
      l.config.b should not equal (initial.b)
      l.config.b should equal("rocky")

      // And, of course, the required field is set
      l.config.required should equal("satsuma")
    }

    "overridden fields will not be written if the config file also specifies the key" in {
      writeConfig(HashMap[String, Any]("b" -> "goat", "required" -> "satsuma"))

      val l = newLoader.load(cPath, Map("b" -> "rocky"))
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      // Because the config file has a value specified for key `b`, it must always
      // take priority even if there's an
      // overridden field for this loader.
      l.config.b should not equal ("rocky")
      l.config.b should equal("goat")

      // And, of course, the required field is set
      l.config.required should equal("satsuma")
    }

    "works for small longs" in {
      writeConfig(HashMap[String, Any]("bigger" -> 3, "required" -> "satsuma"))

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      l.config.bigger shouldBe 3
    }

    "works for big longs" in {
      writeConfig(
        HashMap[String, Any](
          "bigger" -> (Int.MaxValue.longValue + 1),
          "required" -> "satsuma"))

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      l.config.bigger shouldBe Int.MaxValue.longValue + 1
    }

    "works for list of longs" in {
      writeConfig(
        HashMap[String, Any]("longs" -> Seq(1, 2, 3), "required" -> "satsuma"))

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      // This uses scala magic to cast the types, so it doesn't actually test all
      // that much.
      l.config.longs shouldBe List(1, 2, 3)

      // `foreach` boxes up the long, so it'll blow up if the value isn't actually a
      // long.
      l.config.longs.foreach { l =>
        l shouldBe a[Long]
      }
    }

    "works for list of strings" in {
      writeConfig(
        HashMap[String, Any](
          "strings" -> Seq("hello", "world"),
          "required" -> "satsuma"))

      val l = newLoader.load(cPath)
      l.warnings should equal(Nil)
      l.errors should equal(Nil)

      l.config.strings shouldBe List("hello", "world")
    }
  }
}
