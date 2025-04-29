package fauna.tools

import fauna.api.test.APISpecMatchers
import fauna.prop.api._
import io.netty.buffer.ByteBuf
import java.io._
import java.lang.{ ProcessBuilder => JProcessBuilder }
import java.util.stream.{ Stream => JStream }
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.{ Future, Promise }

package test {

  trait Spec extends AnyFreeSpec
      with BeforeAndAfterAll
      with Matchers
      with APISpecMatchers
      with APIResponseHelpers
      with DefaultQueryHelpers
      with OptionValues
      with Eventually {

    val rootKey = FaunaDB.rootKey

    private val classpath = getSysProp("fauna.RunCore.classpath")

    def launchTool(
      main: String,
      flags: Map[String, String] = Map.empty,
      args: List[String] = Nil,
      input: Option[ByteBuf] = None,
      andStderr: Boolean = true,
      config: Option[String] = None): Future[(Int, JStream[String])] = {
      val pargs = List.newBuilder[String]

      pargs ++= Seq("java", "-server", "-cp", classpath)

      pargs ++= Seq(
        "--add-opens", "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens", "java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens", "java.base/java.util=ALL-UNNAMED",
        "--add-opens", "java.base/java.io=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        main)

      pargs ++= args

      flags foreach {
        case (flag, value) => pargs ++= Seq(s"-$flag", value)
      }

      val result = Promise[(Int, JStream[String])]()

      new Thread(() => {
        try {
          val pb = new JProcessBuilder(pargs.result(): _*)
            .redirectErrorStream(andStderr)
          pb.environment().remove("FAUNADB_CONFIG")
          pb.environment().remove("FAUNADB_ROOT_KEY")
          config foreach { c => pb.environment().put("FAUNADB_CONFIG", c) }
          val process = pb.start
          input foreach { buf =>
            val output = process.getOutputStream
            buf.readBytes(output, buf.readableBytes())
            output.close()
          }
          val lines = getProcessStream(process.getInputStream)
          result.success((process.waitFor(), lines))
        } catch {
          case t: Throwable => result.failure(t)
        }
      }).start()

      result.future
    }

    private def getProcessStream(is: InputStream): JStream[String] =
      new BufferedReader(new InputStreamReader(is)).lines()

    private def getSysProp(key: String): String =
      Option(System.getProperty(key)) match {
        case Some(value) => value
        case None =>
          throw new IllegalStateException(
            s"$key system property not set! Your project should declare .enablePlugins(fauna.sbt.RunCoreConfigExportPlugin)")
      }
  }
}
