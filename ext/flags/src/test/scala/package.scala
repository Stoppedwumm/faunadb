package fauna.flags

import fauna.prop.test.PropSpec
import java.nio.file.{ Files, Path, Paths }
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers

package test {
  abstract class Spec
      extends PropSpec(10, 100)
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Matchers
      with Eventually {

    // System.setProperty("io.netty.leakDetection.targetRecords", "60")
    // import io.netty.util.ResourceLeakDetector
    // ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)

    private val ramRoot = {
      val defaultRoot = Option(System.getenv("FAUNA_TEST_ROOT"))
      val base = System.getProperty("os.name") match {
        case "Linux"    => defaultRoot getOrElse "/dev/shm"
        case "Mac OS X" => defaultRoot getOrElse "/Volumes"
        case x if x.startsWith("Windows") =>
          defaultRoot getOrElse System.getProperty("java.io.tmpdir")
        case _ => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
      }

      Paths.get(base, "fauna-api-test", "flags")
    }

    def aTestDir(
      ram: Boolean = true,
      label: String = getClass.getSimpleName): Path = {
      val prefix = s"$label-"
      if (ram) {
        Files.createDirectories(ramRoot)
        Files.createTempDirectory(ramRoot, prefix)
      } else {
        Files.createTempDirectory(prefix)
      }
    }
  }
}
