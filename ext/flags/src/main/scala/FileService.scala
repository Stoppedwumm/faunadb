package fauna.flags

import fauna.codex.cbor.CBOR
import fauna.codex.json._
import fauna.exec.LoopThreadService
import fauna.lang.syntax.getLogger
import fauna.stats.StatsRecorder
import fauna.stats.StatsRecorder.Null
import java.nio.file._
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

/** Contains a property name, value, and a list of flags to set if that property
  * set matches.
  *
  * For example, of `name` were `host_id`, and `value` were `1234`, then all the
  * flags in the `flags` map would be applied to the host with id `1234`.
  */
private final case class Property(
  name: String,
  value: Value,
  flags: Map[String, Value])

private object Property {
  implicit object Decoder extends JsonDecoder[Property] {
    override def decode(stream: JsonCodec.In): Property = {
      Property(
        name = (stream / "property_name").as,
        value = (stream / "property_value").as,
        flags = (stream / "flags").as
      )
    }
  }
}

/** Contains the full state of the `flags.json` file.
  */
private[flags] final case class State(version: Int, properties: Seq[Property]) {
  def flagsForValues(values: Map[String, Value]) = {
    properties.flatMap { storedProp =>
      val value = values.get(storedProp.name)
      // If the value is in the store, but not in the queried props, we skip
      // it.
      if (value.contains(storedProp.value)) {
        storedProp.flags
      } else {
        Map.empty
      }
    }.toMap
  }
}

private[flags] object State {
  val AccountFlagPrefix = "FLAG_ACCOUNT_"

  implicit object Decoder extends JsonDecoder[State] {
    override def decode(stream: JsonCodec.In): State = {
      State(
        version = (stream / "version").as,
        properties = (stream / "properties").as
      )
    }
  }

  def load(path: Path): Try[State] = {
    Try(Files.readAllBytes(path)) flatMap { buf =>
      JsonCodec.decode[State](buf) map { state =>
        require(state.version >= 0, "State cannot have a negative version")
        state
      }
    }
  }

  def fromEnv(): State = {
    State(
      -1,
      sys.env.view.flatMap {
        case (flag, value) if flag.startsWith(AccountFlagPrefix) =>
          val name = flag.stripPrefix(AccountFlagPrefix).toLowerCase
          val v = Value.fromString(value)

          Some(Property(name = "account_id", value = 0, flags = Map(name -> v)))
        case _ => None
      }.toSeq
    )
  }
}

/** Calls `thunk` when the file at `path` gets updated. `path` must be the path
  * to the file to watch. `FileWatcher` will setup a java file watcher to watch
  * the parent of this path.
  */
private final class FileWatcher(path: Path, thunk: WatchEvent[_] => Unit) {
  private val log = getLogger()

  private[this] val parent = if (path.getParent eq null) {
    Path.of(".")
  } else {
    path.getParent
  }
  private[this] val watcher = FileSystems.getDefault.newWatchService()
  parent.register(
    watcher,
    StandardWatchEventKinds.ENTRY_CREATE,
    StandardWatchEventKinds.ENTRY_MODIFY,
    StandardWatchEventKinds.ENTRY_DELETE)

  /** Polls for events. Returns `true` if the watcher is still valid. If this
    * returns `false`, the watcher is no longer valid.
    */
  def poll(): Boolean =
    try {
      val key = watcher.take()
      key.pollEvents.forEach { event =>
        // event.context is a sun.nio.fs.UnixPath on linux. The docs don't
        // specify what event.context should be, but I'm assuming it's something
        // that can be compared to a Path. So I'm just going to use `== path` and
        // hope for the best.
        //
        // Additionally, the path stored in event.context is relative to the
        // watched directory. Because we always just watch the parent, the context
        // will just be the filename of the path we are watching.
        if (event.context == path.getFileName) {
          try {
            thunk(event)
          } catch {
            case NonFatal(err) => log.error(err)
          }
        }
      }
      key.reset()
    } catch {
      case _: ClosedWatchServiceException => false
    }

  def close(): Unit = {
    watcher.close()
  }
}

object FileService {
  private[flags] val EnvState = State.fromEnv()
}

/** This is a file-based feature flags service. It exposes a few public
  * functions:
  *
  * - start: Starts the LoopThreadService, and this will poll for updates on
  *   disk once every 10 seconds.
  * - stop: Stops polling for updates on disk.
  * - getAllUnached: Performs a flag lookup on the current loaded state of the flags.
  *
  * If the flags are invalid on disk, the LoopThreadService will continue to
  * run, and flags will attempt to be loaded once every 10 seconds, in order to
  * recover if the file reappears.
  *
  * Any time a new file is found, the version must be greater than the current
  * loaded version. If the file contains an older version than what is currently
  * loaded, then the file will be ignored.
  *
  * This service will emit two metrics:
  * - FFService.CurrentVersion: This is emitted every time a new file is loaded.
  *   It will contain the value of the current state's version. This will be -1
  *   if the file was not present on startup.
  * - FFService.FilePresent: This will be `1` if the file is present, and `0` if
  *   the file is missing. This will be used to page if the file has been
  *   missing for too long.
  */
final class FileService(val path: Path, stats: StatsRecorder = Null)
    extends LoopThreadService("FFFileService at " + path.toString)
    with Service {
  private val log = getLogger()

  /** state is volatile, and the State class is immutable, so this is thread-safe */
  def version = state.version

  override def getAllUncached[ID, P <: Properties[ID], F <: Flags[ID]](
    props: Vector[P])(
    implicit propCodec: CBOR.Encoder[Vector[P]],
    flagsCodec: CBOR.Decoder[Vector[F]],
    companion: FlagsCompanion[ID, F]): Future[Vector[F]] = {
    require(isRunning, "feature flag service is not running")
    val currentState = state
    Future.successful(
      props map { prop =>
        val values = prop.values
        // This loop is essentially a ruleset. The last flag in the list is what
        // will be used if multiple rules (storedProp) match the query. For
        // example, if currentState.properties looked like this:
        //
        // - property_name: "replica_name"      this is storedProp.name
        //   property_value: "NoDC"             this is storedProp.value
        //   flags:                             this is storedProp.flags
        //     my_flag: 10000
        // - property_name: "host_id"
        //   property_value: 1234
        //   flags:
        //     my_flag: 3
        //
        // Then, for a host with id `1234`, in the replica `NoDC`, `my_flag`
        // would be 3, because it is the last match in the list.
        //
        // FIXME: Use a more efficient structure, so that we don't need
        // to iterate through every rule for every flag lookup.
        val envFlags = FileService.EnvState.flagsForValues(values)
        val liveFlags = currentState.flagsForValues(values)
        companion(prop.id, envFlags ++ liveFlags)
      }
    )
  }

  @volatile
  private[this] var state: State = State(-1, Seq.empty)
  @volatile
  private[this] var filePresent: Boolean = false
  private[this] var watcher: Option[FileWatcher] = None

  StatsRecorder.polling {
    stats.set("FFService.CurrentVersion", state.version)
    stats.set(
      "FFService.Running",
      if (watcher.isDefined) {
        1
      } else {
        0
      })
    stats.set(
      "FFService.FilePresent",
      if (filePresent) {
        1
      } else {
        0
      })
  }

  /** Called whenever an update to the flags.json file happens. This will emit
    * logs/metrics for a missing file, and read a new state if the file has been
    * updated.
    */
  private def onEvent(ev: WatchEvent[_]) = {
    ev.kind match {
      case StandardWatchEventKinds.ENTRY_CREATE =>
        stats.incr("FFService.Events.Create")
      case StandardWatchEventKinds.ENTRY_MODIFY =>
        stats.incr("FFService.Events.Modify")
      case StandardWatchEventKinds.ENTRY_DELETE =>
        stats.incr("FFService.Events.Delete")
      case _ => log.error(s"Unkown watcher event: $ev")
    }
    if (ev.kind == StandardWatchEventKinds.ENTRY_DELETE) {
      setMissing()
    } else {
      setPresent()
      log.info(s"FF: Found new state, loading...")
      State.load(path) match {
        case Success(newState) =>
          val currentState = state
          if (newState.version > currentState.version) {
            log.info(
              s"FF: Loaded new flags with version ${newState.version} (previous version: ${currentState.version})")
            state = newState
          } else {
            log.error(
              s"FF: Ignoring new feature flags, as new version ${newState.version} is <= the current version ${currentState.version}")
          }
        case Failure(error) =>
          log.error("Failed to load feature flags file", error)
      }
    }
  }

  /** Called whenever the file is missing. This will emit a log and metric, for
    * visibility when the file disapears.
    */
  private def setMissing(): Unit = {
    log.warn(s"FF: Feature flags deleted from disk: $path")
    filePresent = false
  }

  /** Called whenever the file is present. This will emit a log and metric, for
    * visibility when the file is present.
    */
  private def setPresent(): Unit = {
    log.info(s"FF: Feature flags recreated on disk: $path")
    filePresent = true
  }

  /** Starts the flags service. This will also start the file watcher. If the
    * file is missing from disk, this will start with no state loaded. If the
    * directory containing the file is missing from disk, this function will
    * throw an exception.
    */
  override def start(): Unit = {
    if (watcher.isDefined) {
      throw new IllegalStateException("FF: File service is already running")
    }
    watcher = Try(new FileWatcher(path, onEvent)) match {
      case Success(watcher) =>
        log.info(s"FF: Feature flags watcher has been setup to watch $path")
        Some(watcher)
      case Failure(error) =>
        log.error("Could not setup FileWatcher", error)
        setMissing()
        throw error
    }
    state = State.load(path) match {
      case Success(state) =>
        log.info(s"FF: Loaded initial feature flags with version ${state.version}")
        setPresent()
        state
      case Failure(error) =>
        log.error("Invalid Feature Flags", error)
        setMissing()
        State(-1, Seq.empty)
    }
    super.start()
  }

  /** This will poll for any updates to the flags file, and then wait 10
    * seconds.
    */
  override protected def loop(): Unit = {
    require(watcher.isDefined, "FF: File service was not started")
    val w = watcher.get
    val valid = w.poll()
    _continue = valid
    if (!valid) {
      setMissing()
      watcher = None
      log.error(s"FF: Feature flags watcher has become invalid at $path")
    }

    // Only check for updates once every 10 seconds
    Thread.sleep(10_000)
  }

  /** Stops the flags service. This will close this file watcher, if it is open.
    */
  override def stop(): Unit = {
    if (watcher.isDefined) {
      val w = watcher.get
      w.close()
    }
    watcher = None
    super.stop()
  }
}
