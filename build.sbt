import fauna.sbt.TaskHelpers._

val api = project in file("ext/api")
val atoms = project in file("ext/atoms")
val cassandra = project in file("ext/cassandra")
val cluster = project in file("ext/cluster")
val codex = project in file("ext/codex")
val config = project in file("ext/config")
val exec = project in file("ext/exec")
val flags = project in file("ext/flags")
val fql = project in file("ext/fql")
val fuzz = project in file("ext/fuzz")
val lang = project in file("ext/lang")
val limits = project in file("ext/limits")
val logging = project in file("ext/logging")
val model = project in file("ext/model")
val multicore = project in file("ext/multicore")
val net = project in file("ext/net")
val prop = project in file("ext/prop")
val propAPI = project in file("ext/prop-api")
val repair = project in file("ext/repair")
val repo = project in file("ext/repo")
val scheduler = project in file("ext/scheduler")
val snowflake = project in file("ext/snowflake")
val stats = project in file("ext/stats")
val storage = project in file("ext/storage")
val storageAPI = project in file("ext/storage-api")
val tools = project in file("ext/tools")
val trace = project in file("ext/trace")
val tx = project in file("ext/tx")

val service = project
val qa = project

val jmh = project.enablePlugins(JmhPlugin).dependsOn(codex, flags, repo, model)

val mainProjects = Seq(service, qa, jmh)

val extProjects = Seq(
  api,
  atoms,
  qa,
  cassandra,
  cluster,
  codex,
  config,
  exec,
  flags,
  fql,
  fuzz,
  lang,
  logging,
  model,
  multicore,
  net,
  prop,
  propAPI,
  repair,
  repo,
  scheduler,
  snowflake,
  stats,
  storage,
  storageAPI,
  tools,
  trace,
  tx
)

val allProjects = (mainProjects ++ extProjects) map { p => LocalProject(p.id) }

// root project

val root = (project in file("."))
  .aggregate(allProjects: _*)
  .settings(
    // adjust aggregation
    (test / aggregate) := false,
    (testOnly / aggregate) := false,
    (testQuick / aggregate) := false,
    (assembly / aggregate) := false,

    // disable global assembly
    assembly := sys.error("use service/assembly"),

    // simpler serialized test runs
    test := (serializedAggregate(
      allProjects
        // Do not run multicore tests from the root.
        .filterNot { _.project == "multicore" }
        .map { (_ / Test / test) })).value,
    (Global / coreClasspath) := (service / Compile / fullClasspath).value
  )
