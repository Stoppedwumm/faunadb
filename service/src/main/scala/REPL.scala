package fauna.util

import fauna.lang.syntax._
import java.io._
import java.net._
import scala.tools.nsc._
import scala.tools.nsc.interpreter.shell._

/**
 * A trait to ease the embedding of the scala interpreter
 */

class REPLContext(val in: BufferedReader, val out: PrintWriter) {
  def println(o: Any) = out.println(o)
  def println() = out.println()
}

object REPL {
  def apply(pr: String, help: String = "", welcome: String = ""): REPL = new REPL {
    val prompt = pr
    val helpMsg = help
    val welcomeMsg = welcome
  }
}

trait REPL {
  def prompt: String
  def helpMsg: String
  def welcomeMsg: String

  private var bindings: Map[String, (String, Any, List[String])] = Map.empty

  private var imports: List[String] = List.empty

  def bind[A <: AnyRef](name: String, value: A)(implicit m: Manifest[A]): Unit =
    bindAs(name, value, m.runtimeClass.getCanonicalName)

  def bindAs(name: String, value: Any, typeName: String): Unit =
    bindings += ((name, (typeName, value, Nil)))

  def bindImplicit[A <: AnyRef](name: String, value: A)(implicit m: Manifest[A]): Unit =
    bindImplicitAs(name, value, m.runtimeClass.getCanonicalName)

  def bindImplicitAs(name: String, value: Any, typeName: String): Unit =
    bindings += ((name, (typeName, value, List("implicit"))))

  def autoImport(importString: String): Unit =
    imports = importString :: imports

  class Loop(c: ShellConfig, i: BufferedReader, o: PrintWriter) extends ILoop(c, i, o) {
    override lazy val prompt = REPL.this.prompt

    val ctx = new REPLContext(i, o)

    override def createInterpreter(settings: Settings): Unit = {
      super.createInterpreter(settings)

      intp beQuietDuring {
        intp.bind("REPL" -> ctx)
        intp.interpret("import REPL.println")

        for ((name, (clazz, value, modifiers)) <- bindings) {
          intp.bind(name, clazz, value, modifiers)
        }
        for (i <- imports) {
          intp.interpret(s"import $i")
        }
      }
    }

    override def helpCommand(line: String): Result = {
      if (line == "") echo(helpMsg)
      super.helpCommand(line)
    }

    override def printWelcome(): Unit = {
      out.println(welcomeMsg)
      out.flush()
    }
  }

  def run() = {
    val in = new BufferedReader(new InputStreamReader(System.in))
    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)))

    attach(in, out)
  }

  def attach(in: BufferedReader, out: PrintWriter): Unit = {
    val config = new GenericRunnerSettings(out.println)
    config.usejavacp.value = true
    val cfg = ShellConfig(config)

    (new Loop(cfg, in, out)).run(config)
  }
}

class REPLServer(repl: REPL, address: String, port: Int) {
  private val log = getLogger

  @volatile private var thread: Thread = null

  def start(): Unit = {
    synchronized {
      if (thread eq null) {
        log.info(s"Starting console listener on $address:$port.")
        thread = new Thread("REPL listener") {
          override def run() = {
            val server = new ServerSocket(port, 0, InetAddress.getByName(address))
            var idx = 0

            server.setSoTimeout(1000)

            while (!isInterrupted) {
              try {
                val sock = server.accept()
                idx += 1

                val session = new Thread(null, null, s"REPL session $idx", 1 * 1024 * 1024) {
                  override def run() = {
                    log.info(s"Starting console session for ${sock.getRemoteSocketAddress}")
                    sock.setTcpNoDelay(true)

                    val in = new BufferedReader(new InputStreamReader(sock.getInputStream()))
                    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(sock.getOutputStream())))

                    repl.attach(in, out)
                    log.info(s"Exited console session for ${sock.getRemoteSocketAddress}")
                  }
                }

                session.start()

              } catch {
                case _: SocketTimeoutException => ()
              }
            }

            server.close()

            log.info("Console listener stopped.")
          }
        }

        thread.start()
      }
    }
  }

  def stop(): Unit = {
    synchronized {
      if (thread ne null) {
        log.info("Stopping console listener.")
        thread.interrupt
        thread.join()
        thread = null
      }
    }
  }
}
