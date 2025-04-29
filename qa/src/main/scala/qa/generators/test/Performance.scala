package fauna.qa.generators.test

import fauna.codex.json.{ JSString, JSTrue }
import fauna.prop.api.JSGenerators
import fauna.qa._
import java.util.concurrent.atomic.AtomicInteger

/**
  * Run a set of work loads in sequence. Used for gathering performance
  * information.
  */
class Performance(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new PerformanceWorkloadGenerator("perfworkload", config))
}

/**
  * A configurable workload consisting of basic read/write/updates.  The shape
  * of the data is controlled by these configs:
  *
  *   1. records       The number of documents to load
  *   2. fieldlength   The lenght of each field
  *   3. fieldcount    The number of fields  (TBD)
  *   4. batchSize     The number of creates in a single transaction for the load
  *
  * Each stream will randomly generate a workload.  The workload can be configured
  * by the following:
  *    1.  tx.minsize       The minimum number of fql operations in a transaction (Inclusive)
  *    2.  tx.maxsize       The maximum number of fql operations in a transaction (Exclusive)
  *    3.  tx.reads         The percentage of read(Get) operations
  *    4.  tx.updates       The percentage of update operations
  *    5.  tx.writes        The percentage of writes(create) operations
  */
class PerformanceWorkloadGenerator(name: String, conf: QAConfig)
    extends TestGenerator(name, conf)
    with JSGenerators {

  val dataintegrity = conf.getBoolean("performance.dataintegrity") //TBD
  val records = conf.getInt("performance.records")
  val insertBatchSize = conf.getInt("performance.batchsize")
  val fieldCount = conf.getInt("performance.fieldcount") // TBD
  val fieldLength = conf.getInt("performance.fieldlength")

  val txTotal = conf.getDouble("performance.tx.reads") +
    conf.getDouble("performance.tx.updates")
  val txReads = conf.getDouble("performance.tx.reads") / txTotal
  val txUpdates = conf.getDouble("performance.tx.updates") / txTotal
  val txWrites = conf.getDouble("performance.tx.writes") // TBD
  val txDeletes = conf.getDouble("performance.tx.deletes") // TBD
  val txMinSize = conf.getInt("performance.tx.minsize")

  val txMaxSize = if (conf.getInt("performance.tx.maxsize") > txMinSize) {
    conf.getInt("performance.tx.maxsize")
  } else {
    // If we have messed use a correct default
    log.warn(s"Invalid performance.tx.maxsize set, now using ${txMinSize + 1}")
    txMinSize + 1
  }

  var numReads: Long = 0
  var numUpdates: Long = 0
  var numWrites: Long = 0
  var numTransactions: Long = 0
  var numTransactionsReported: Long = 0

  val totalRecords = new AtomicInteger(0)
  val collectionName = "usertable"
  val collectionRef = ClassRef(collectionName)
  val rand = config.rand

  sealed abstract class OpType(override val toString: String)

  object OpType {
    case object Read extends OpType("read")
    case object Update extends OpType("update")
    case object Write extends OpType("write")
  }

  val schema = Schema.DB(
    name,
    Schema.Collection(collectionName)
  )

  override def initializer(schema: Schema.DB, clientID: Int) = {
    val auth = schema.serverKey.get
    log.info(
      s"Workload R:${txReads} W:${txWrites} U:${txUpdates} Batch:${insertBatchSize}")

    new RequestIterator {
      override val name: String = "init"

      override def hasNext: Boolean = totalRecords.get() < records

      override def next(): FaunaQuery =
        FaunaQuery(auth, (1 to insertBatchSize).toList map { _ =>
          write()
        })
    }
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("workload", () => buildTransaction(auth))
  }

  def makeDataObject(op: OpType) = {
    MkObject(
      "data" -> MkObject(
        "field1" -> jsString(fieldLength, fieldLength).sample,
        "operation" -> JSString(op.toString + System.currentTimeMillis().toString)
      ))
  }

  private def write() = {
    numWrites += 1
    val id = totalRecords.incrementAndGet()
    log.debug(s"${OpType.Write}(id${id})")
    If(Exists(MkRef(collectionRef, id)),
       JSTrue,
       CreateF(MkRef(collectionRef, id), makeDataObject(OpType.Write)))
  }

  private def read() = {
    numReads += 1
    val id = idChooser()
    log.debug(s"${OpType.Read}(id${id})")
    Get(MkRef(collectionRef, id))
  }

  private def update() = {
    numUpdates += 1
    val id = idChooser()
    val data = makeDataObject(OpType.Update)
    log.debug(s"${OpType.Update}(id${id})")
    Update(MkRef(collectionRef, id), data)
  }

  //  Return a random id avoiding 0
  def idChooser(): Long = rand.between(1, records)

  //  Return the operation to run next
  def operationChooser() =
    if (rand.nextDouble() < txUpdates) {
      update()
    } else {
      read()
    }

  def buildTransaction(auth: String): FaunaQuery = {
    numTransactions += 1
    val fql = (1 to rand.between(txMinSize, txMaxSize)) map { _ =>
      operationChooser()
    }

    if (numTransactions >= numTransactionsReported) {
      // report approx every 10K transactions
      numTransactionsReported += 10000
      log.info(
        s"Stats nTxn $numTransactions nReads $numReads  nUpdates $numUpdates  nWrites $numWrites")
    }

    log.debug(
      s"fql transaction ${numTransactions} operations:\n${fql.mkString(", ")}")

    FaunaQuery(auth, fql)
  }
}
