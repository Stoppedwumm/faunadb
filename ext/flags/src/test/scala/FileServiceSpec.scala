package fauna.flags.test

import fauna.atoms.HostID
import fauna.flags._
import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, NoSuchFileException, Path, StandardWatchEventKinds }
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await

class FileServiceSpec extends Spec {
  val hostID = HostID.randomID
  def hostProps(): HostProperties = {
    HostProperties(
      hostID,
      Map(
        Properties.HostID -> hostID.toString,
        Properties.Environment -> "prod",
        Properties.RegionGroup -> "us-std",
        Properties.ReplicaName -> "my-replica",
        Properties.ClusterName -> "dev.us-std.default"
      )
    )
  }

  def getHostFlags(svc: Service): HostFlags = {
    val flags = Await.result(
      svc.getAllUncached[HostID, HostProperties, HostFlags](Vector(hostProps())),
      1.second)
    flags.size should be(1)
    flags(0)
  }

  test("works") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "my-flag" -> true,
            "other-flag" -> 3
          )
        )
      ))

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "my-flag" -> true,
          "other-flag" -> 3
        )))

    svc.stop()
  }

  test("flags can load without a flags.json file") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    // This should work, since the directory exists, but the file doesn't
    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(HostFlags(hostID, Map()))

    svc.stop()
  }
  test("flags cannot load without a flags directory") {
    val flagsPath = Path.of("/tmp/foo/this-really-shouldn't-exist/flags.json")

    // This shouldn't crash even though there is no file or directory.
    val svc = new FileService(flagsPath)
    assertThrows[NoSuchFileException] {
      svc.start()
    }
  }

  test("reads replica only flags") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          "replica_name",
          "my-replica",
          Map(
            "foo-flag" -> true,
            "other-flag" -> 3
          )
        )
      ))

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "foo-flag" -> true,
          "other-flag" -> 3
        )))

    svc.stop()
  }

  test("only reads the flags we care about") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          Properties.ReplicaName,
          "my-replica",
          Map(
            "other-flag" -> 10,
            "base-flag" -> 5
          )
        ),
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            // This will override the above `10`
            "other-flag" -> 3,
            "my-flag" -> true
          )
        ),
        // This does not apply to us, as it's another host.
        FlagProps(
          "host_id",
          HostID.randomID.uuid.toString,
          Map(
            "my-flag" -> false,
            "other-flag" -> 20
          )
        )
      )
    )

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "base-flag" -> 5,
          "my-flag" -> true,
          "other-flag" -> 3
        )))

    svc.stop()
  }

  test("reads updates from disk") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "my-flag" -> true,
            "other-flag" -> 10
          )
        )
      )
    )

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "my-flag" -> true,
          "other-flag" -> 10
        )))

    // Version needs to be greater
    FlagsHelpers.writeFlags(
      flagsPath,
      version = 2,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "my-flag" -> false,
            "new-flag" -> "hey i'm new",
            "other-flag" -> 500
          )
        )
      )
    )

    eventually {
      getHostFlags(svc) should be(
        HostFlags(
          hostID,
          Map(
            "my-flag" -> false,
            "new-flag" -> "hey i'm new",
            "other-flag" -> 500
          )))
    }

    svc.stop()
  }

  test("negative versions are invalid") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = -3,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "my-flag" -> true,
            "other-flag" -> 10
          )
        )
      )
    )

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(HostFlags(hostID, Map()))

    svc.stop()
  }

  test("ignores updates that have the same version") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "my-flag" -> true,
            "other-flag" -> 10
          )
        )
      )
    )

    val svc = new FileService(flagsPath)
    svc.start()

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "my-flag" -> true,
          "other-flag" -> 10
        )))

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(
        FlagProps(
          "host_id",
          hostID.uuid.toString,
          Map(
            "uh-oh-dont-load-this" -> 2
          )
        )
      )
    )

    // Give the background thread time to handle the updated file.
    Thread.sleep(100)

    getHostFlags(svc) should be(
      HostFlags(
        hostID,
        Map(
          "my-flag" -> true,
          "other-flag" -> 10
        )))

    svc.stop()
  }

  test("FileWatcher only reads the correct files") {
    val dir = aTestDir()
    val watchingPath = Path.of(dir.toString + "/foo.txt")
    val otherPath = Path.of(dir.toString + "/bar.txt")

    var events = 0
    val watcher = new FileWatcher(
      watchingPath,
      ev => {
        if (events == 0) {
          ev.kind() should be(StandardWatchEventKinds.ENTRY_CREATE)
        } else {
          ev.kind() should be(StandardWatchEventKinds.ENTRY_MODIFY)
        }
        (ev.context == watchingPath.getFileName) should be(true)
        events += 1
      }
    )

    // The first two events should be a create and modify.
    Files.write(watchingPath, "things".getBytes(StandardCharsets.UTF_8))
    eventually {
      watcher.poll()
      events should be(2)
    }

    // No events should be created for modifying another file.
    Files.write(otherPath, "other things".getBytes(StandardCharsets.UTF_8))
    eventually {
      watcher.poll()
      events should be(2)
    }

    // A third event should be created, which is a modified event.
    Files.write(watchingPath, "more things".getBytes(StandardCharsets.UTF_8))
    eventually {
      watcher.poll()
      events should be(3)
    }

    watcher.close()
  }

  test("cached views are updated on flags change") {
    val dir = aTestDir()
    val flagsPath = Path.of(dir.toString + "/flags.json")

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 1,
      Vector(FlagProps("host_id", hostID.uuid.toString, Map("my-flag" -> "a"))))

    val svc = new FileService(flagsPath)
    svc.start()

    val cachedView =
      svc.getCached[HostID, HostProperties, HostFlags](hostProps())

    Await.result(cachedView.get, 1.second) shouldEqual
      Some(HostFlags(hostID, Map("my-flag" -> "a")))

    FlagsHelpers.writeFlags(
      flagsPath,
      version = 2,
      Vector(FlagProps("host_id", hostID.uuid.toString, Map("my-flag" -> "b"))))

    // Give the background thread time to handle the updated file.
    Thread.sleep(100)

    Await.result(cachedView.get, 1.second) shouldEqual
      Some(HostFlags(hostID, Map("my-flag" -> "b")))

    svc.stop()
  }
}
