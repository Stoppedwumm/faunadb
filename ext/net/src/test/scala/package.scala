package fauna.net

import fauna.logging.test._
import fauna.net.security.PEMFile
import java.nio.file._
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time._

package object test extends NetworkHelpers {
  lazy val log = setupConsoleLogger()
}

package test {
  trait Spec extends AnyFreeSpec
      with BeforeAndAfter
      with BeforeAndAfterAll
      with Eventually
      with PrivateMethodTester
      with Matchers {

    // System.setProperty("io.netty.leakDetection.targetRecords", "60")
    // import io.netty.util.ResourceLeakDetector
    // ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID)

    implicit override val patienceConfig =
      PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

    def testPEMFile(name: String) =
      PEMFile(Paths.get(getClass.getClassLoader.getResource(s"pem_files/$name").toURI))

    def time(reps: Long)(f: => Any): Unit = {
      var i = 0L

      // warmup

      while (i < (reps / 10)) { f; i += 1 }

      // time

      val start = System.nanoTime
      i = 0L

      while (i < reps) { f; i += 1 }

      val end = System.nanoTime
      val total = end - start
      val perRep = total / reps.toDouble

      info("%.2f us total, %.2f us / rep".format(total / 1000.0, perRep / 1000.0))
    }

    private val ramRoot = {
      val defaultRoot = Option(System.getenv("FAUNA_TEST_ROOT"))
      val base = System.getProperty("os.name") match {
        case "Linux" => defaultRoot getOrElse "/dev/shm"
        case "Mac OS X" => defaultRoot getOrElse "/Volumes"
        case x if x.startsWith("Windows") => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
        case _ => defaultRoot getOrElse System.getProperty("java.io.tmpdir")
      }

      Paths.get(base, "fauna-api-test", "net")
    }

    def aTestDir(ram: Boolean = true, label: String = getClass.getSimpleName): Path = {
      val prefix = s"$label-"
      val dir = if (ram) {
        Files.createDirectories(ramRoot)
        Files.createTempDirectory(ramRoot, prefix)
      } else {
        Files.createTempDirectory(prefix)
      }

      log.info(s"Created test tmp dir $dir")
      dir
    }
  }
}
