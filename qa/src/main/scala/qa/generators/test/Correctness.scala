package fauna.qa.generators.test

import fauna.qa.generators.test.correctness.{ TestConfig, TestData }
import fauna.qa.generators.test.CorrectnessGenerator._
import fauna.qa._
import fauna.codex.json._
import fauna.prop._
import fauna.prop.api._
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.util.{ Failure, Success }

/**
  * This adds a Correctness "benchmark" that's verifying operations and logging
  * warnings when expectations are not met. It's designed to randomly create
  * objects in batches, remember a sample of created objects, delete objects
  * in batches, and execute creation batches that are expected to fail as they
  * intentionally contain data in objects that will violate uniqueness constraints.
  */
object CorrectnessGenerator {
  val MaxQueryRetries = 5
  val QueryRetryDelay = 10
  val StatLogIntervalMs = 15000

  object Operation extends Enumeration {
    val Create, Read, Update, Delete, VerifyIndexes, UCVOnCreate, UCVOnDelete = Value
  }
  type Operation = Operation.Value
}

class CorrectnessGenerator(name: String, config: QAConfig)
    extends TestGenerator(name, config)
    with JSGenerators {

  override val expectFailures = true
  val tConfig = new TestConfig(config)

  private var testData: Option[TestData] = None

  def data(auth: String) =
    testData getOrElse {
      val td = new TestData(auth, propConfig, tConfig)
      testData = Some(td)
      td
    }

  val schema = if (tConfig.IndexAndVerify) {
    Schema.DB(
      name,
      Schema.Collection("class1"),
      Schema.Index(
        "index1_u1",
        "class1",
        Vector(Schema.Index.Term(Vector("data", "p1"))),
        Vector.empty,
        true
      ),
      Schema.Index(
        "index1_u2",
        "class1",
        Vector(Schema.Index.Term(Vector("data", "p2"))),
        Vector.empty,
        true
      ),
      Schema.Index(
        "index1_n3",
        "class1",
        Vector(Schema.Index.Term(Vector("data", "p3"))),
        Vector.empty,
        false
      ),
      Schema.Index(
        "index1_n4",
        "class1",
        Vector(Schema.Index.Term(Vector("data", "p4"))),
        Vector.empty,
        false
      ),
      Schema.Index(
        "index1_u34",
        "class1",
        Vector(
          Schema.Index.Term(Vector("data", "p3")),
          Schema.Index.Term(Vector("data", "p4"))
        ),
        Vector.empty,
        true
      )
    )
  } else {
    Schema.DB(name, Schema.Collection("class1"))
  }

  def stream(schema: Schema.DB) = {
    log.info(s"Prop seed = ${config.propConfig.seed}")
    // Seed each stream's prop config with the root prop config
    val seeds = Prop.long.times(tConfig.StreamCount).sample
    val ids = 0 until tConfig.StreamCount

    val streams = (seeds zip ids) map {
      case (s, id) =>
        StatefulRequestStream(schema, id, config.propConfig.copy(seed = s)).createStream
    }

    new MultiStream(streams.toIndexedSeq)
  }

  private case class StatefulRequestStream(
    schema: Schema.DB,
    id: Int,
    propConfig: PropConfig
  ) {
    private implicit val _propConfig = propConfig
    private[this] val knownObjects = new SamplingBuffer(tConfig.MaxRememberedObjects)
    private[this] val knownDeletes = new SamplingBuffer(tConfig.MaxRememberedObjects)

    // A queue of repeatable queries that should be retried at some later time.
    // To preserve determinism, an enqueued query is not delayed for an amount
    // of wall-clock time, but rather until a certain number (QueryRetryDelay)
    // of other queries have executed since it was added to the queue; that's
    // why we keep queryCounter.
    @volatile private[this] var delayedQueries = Queue.empty[RepeatableQuery]
    @volatile private[this] var queryCounter: Long = 0L

    @volatile private var lastStatLog = System.currentTimeMillis()
    private[this] val operationCounts = new Array[Int](Operation.maxId)

    def createStream =
      RequestStream(
        s"correctness-$id",
        () => {
          val now = System.currentTimeMillis()
          if (now - lastStatLog > StatLogIntervalMs) {
            lastStatLog = now
            log.info(
              s"stream $id: queries: $queryCounter, operations: ${Operation.values zip operationCounts}, ${knownObjects.size} known objects, ${knownDeletes.size} known deleted refs"
            )
          }
          queryCounter += 1
          delayedQueries.headOption match {
            case Some(query) if query.readyToExecute =>
              delayedQueries = delayedQueries.dequeue._2
              query
            case _ => createRandomQuery()
          }
        }
      )

    private def createRandomQuery() = {
      // If new operations are added here, TestConfig's TotalRatios field
      // should be updated and new *Ratio and *Threshold fields should be
      // added. The order values are accummulated in *Threshold fields must
      // match the order of their usage in "if" statements here.
      val r = Prop.int(tConfig.TotalRatios).sample
      if (r < tConfig.CreateObjectsThreshold) {
        createObjectsQuery()
      } else if (r < tConfig.UpdateObjectsThreshold) {
        updateObjectsQuery()
      } else if (r < tConfig.DeleteObjectsThreshold) {
        deleteObjectsQuery()
      } else if (r < tConfig.ReadObjectsThreshold) {
        readObjectsQuery()
      } else if (r < tConfig.VerifyIndexesThreshold) {
        verifyIndexesQuery()
      } else if (r < tConfig.CreateWithConstraintViolationThreshold) {
        createWithConstraintViolationQuery()
      } else {
        deleteWithConstraintViolationQuery()
      }
    }

    private def createObjectsQuery() = {
      val auth = schema.serverKey.get
      val creates = if (knownObjects.size < tConfig.MinRememberedObjects) {
        sampleSomeCreationQueries(auth, tConfig.MaxCreatedObjects)
      } else {
        sampleCreationQueries(auth)
      }
      val expected = creates map getDataFromCreateAsObject
      dataVerifyingQuery(auth, "create", false, Operation.Create, creates, expected)
    }

    /**
      * Creates a (possibly repeatable) query that will verify that the successful
      * response matches expected values.
      * @param auth the auth for the db
      * @param kind name of the query kind. Only used in log messages.
      * @param repeatable if true, the query should be retried on failure.
      * @param op the operation, for stats purposes.
      * @param operations queries to execute
      * @param expectedData data expected in responses (a seq of "data"
      *                     dictionaries of response objects)
      * @return the repeatable query object
      */
    private def dataVerifyingQuery(
      auth: String,
      kind: String,
      repeatable: Boolean,
      op: Operation,
      operations: Seq[JSObject],
      expectedData: Seq[JSValue]
    ) = {
      val query = JSArray(operations: _*)

      new RepeatableQuery(auth, query) {
        override def errorResult(code: Int, body: JSObject) = {
          log.warn(s"$kind request failed with $code\n$body\for query\n${this.query}")
          None
        }

        override def resultArray(arr: JSArray) = {
          // Check to ensure the response data matches request data
          val responseData = arr.value map { _ / "data" }
          if (responseData != expectedData) {
            if (!(repeatable && setupNextAttempt())) {
              log.warn(
                s"The data in the response\n${JSArray(responseData: _*)}\ndoesn't match expected data\n${JSArray(expectedData: _*)}\nfor $kind query\n${this.query}"
              )
            }
          } else {
            val responseObjs = filterObjects(arr.value)
            countOperations(op, responseObjs.size)
            addKnownObjects(responseObjs)
          }
          None
        }
      }
    }

    private def countOperations(op: Operation, count: Int) =
      operationCounts(op.id) += count

    private def addKnownObjects(objs: Seq[JSObject]) =
      knownObjects.add(objs)

    private def filterObjects(values: Seq[JSValue]) =
      values flatMap { v =>
        v.tryAs[JSObject] match {
          case Success(obj) => Some(obj)
          case Failure(_) =>
            log.warn(
              s"Unexpected response (object expected, got $v instead):\n$values"
            )
            None
        }
      }

    private def getDataFromCreate(create: JSObject) =
      create / "params" / "quote" / "data"

    private def getDataFromCreateAsObject(create: JSObject) =
      getDataFromCreate(create).asInstanceOf[JSObject]

    private def sampleCreationQueries(auth: String) =
      sampleSomeCreationQueries(auth, Prop.int(tConfig.MaxCreatedObjects).sample + 1)

    private def sampleSomeCreationQueries(auth: String, count: Int) = {
      data(auth).class1.times(count).sample map { v =>
        sanitize(v.toRequest.query, false).get.asInstanceOf[JSObject]
      } filter { create =>
        !getDataFromCreate(create).isEmpty
      }
    }

    // Replaces high-contention values (null, booleans, and strings shorter than
    // 3 characters) with strings at least 3 characters long to reduce the
    // chances of unique constraint violations on create and update operations.
    // Also, removes '@' from beginning of key names in "data" in order not to
    // trigger "@obj" wrapping in responses.
    private def sanitize(js: JSValue, stripAt: Boolean): Option[JSValue] = js match {
      case obj: JSObject =>
        Some(JSObject(obj.value flatMap {
          case (k, v) =>
            sanitize(v, stripAt || k == "data") map { (stripAtSign(k, stripAt), _) }
        }: _*))
      case arr: JSArray =>
        Some(JSArray(arr.value flatMap { v =>
          sanitize(v, stripAt)
        }: _*))
      case JSBoolean(_) | JSNull       => someRandomString
      case JSString(s) if s.length < 3 => someRandomString
      case v                           => Some(v)
    }

    private def someRandomString = Some(jsString(3, 20).sample)

    private def stripAtSign(s: String, strip: Boolean) =
      if (strip && s.startsWith("@")) s.substring(1) else s

    private def deleteObjectsQuery() = {
      val auth = schema.serverKey.get

      val objs = takeKnownObjects(tConfig.MaxDeletedObjects)
      val refs = objs map { _ / "ref" }
      val query = JSArray(refs map { ref =>
        DeleteF(ref)
      }: _*)
      new FaunaArrayQuery(auth, query) {
        override def resultArray(arr: JSArray) = {
          val deletedRefs = arr.value map { _ / "ref" }
          if (deletedRefs != refs) {
            log.warn(
              s"The refs in the delete request\n${this.query}\nand response\n$arr\ndon't match"
            )
            None
          } else {
            countOperations(Operation.Delete, refs.size)
            knownDeletes.add(filterObjects(deletedRefs))
            Some(
              makeIndexVerificationQueries(
                auth,
                objs,
                false,
                "survived their objects being deleted"
              )
            )
          }
        }
      }
    }

    /**
      * Creates a query that verifies if values coming from a set of objects are present
      * (or absent, for deleted objects) in indices.
      * @param auth the current db auth
      * @param objs the objects in question
      * @param shouldExist true if the values should be present in the indices, false if
      *                    they should be absent.
      * @param desc a description of the undesired behavior, used to log an indexing failure.
      * @return a repeatable query that when executed performs an array of "exists" queries
      *         based on values in objects.
      */
    private def makeIndexVerificationQueries(
      auth: String,
      objs: Seq[JSObject],
      shouldExist: Boolean,
      desc: String
    ) = {
      val (queries, nullTerms) = makeIndexLookupQueries(objs).unzip

      val indexCheckQuery = JSArray(queries: _*)
      new RepeatableQuery(auth, indexCheckQuery) {
        override def resultArray(arr: JSArray) = {
          // If all terms are null, the entry is not indexed.
          val expectedResponse = nullTerms map { nullTerm =>
            JSBoolean(shouldExist && !nullTerm)
          }
          if (arr.value != expectedResponse) {
            if (!setupNextAttempt()) {
              val unexpected =
                (queries zip (arr.value zip expectedResponse)) filter {
                  case (_, (a, e)) => a != e
                }
              log.warn(
                s"Some unique index entries $desc. Queries with unexpected results:\n$unexpected\nObjects in question:\n$objs"
              )
            }
          } else if (shouldExist) {
            countOperations(Operation.VerifyIndexes, objs.size)
            addKnownObjects(objs)
          }
          None
        }
      }
    }

    // A FaunaArrayQuery that can be repeatedly executed. Used for read queries that
    // check state that is supposed to be eventually consistent, so if it is not
    // consistent at an earlier attempt, a later attempt will be made.
    abstract class RepeatableQuery(authKey: String, query: JSArray)
        extends FaunaArrayQuery(authKey, query) {
      private[this] var attemptsLeft = MaxQueryRetries
      // Logical time when next to execute this query, expressed in terms of
      // StatefulRequestStream.queryCounter.
      private[this] var executeAt: Long = 0L

      def readyToExecute = executeAt < queryCounter

      def setupNextAttempt() = {
        if (attemptsLeft > 0) {
          attemptsLeft -= 1
          executeAt = queryCounter + QueryRetryDelay
          delayedQueries = delayedQueries.enqueue(this)
          true
        } else {
          false
        }
      }
    }

    /**
      * Takes a seq of objects and returns a set of all "exists" queries that can be
      * performed on indices with its values as terms.
      * @param objects the objects to create "exists" queries on
      * @return a seq of tuples of (JSObject, Boolean) where each JSObject is an "exists"
      *         query and the boolean is true if the value is null (or in case of
      *         multi-term index, all values are null).
      */
    private def makeIndexLookupQueries(objects: Seq[JSObject]) = {
      val builder = Seq.newBuilder[(JSObject, Boolean)]
      objects map { _.get("data").as[JSObject] } foreach { data =>
        for (t1 <- findTerms(data, "p1"))
          builder += makeExistsIndexQuery("u1", t1, t1 == JSNull)
        for (t2 <- findTerms(data, "p2"))
          builder += makeExistsIndexQuery("u2", t2, t2 == JSNull)
        for {
          t3 <- findTerms(data, "p3")
          t4 <- findTerms(data, "p4")
        } builder += makeExistsIndexQuery(
          "u34",
          JSArray(t3, t4),
          t3 == JSNull && t4 == JSNull
        )
      }
      builder.result()
    }

    private def makeExistsIndexQuery(
      name: String,
      term: JSValue,
      singleNullTerm: Boolean
    ) =
      (Exists(Match(Ref(s"indexes/index1_$name"), term)), singleNullTerm)

    /**
      * Given an object and a property name, returns all terms that the indexer is
      * expected to produce for the property value. Basically, flattens arrays and
      * replaces objects with nulls.
      * @param obj the object with a property
      * @param name the name of an property
      * @return a sequence (can be empty) with all indexable terms derived from the
      *         property value.
      */
    private def findTerms(obj: JSObject, name: String) =
      obj.value find { case (k, _) => k == name } map {
        case (_, v)                => flattenTerms(v)
      } getOrElse Seq.empty

    private def flattenTerms(v: JSValue): Seq[JSValue] = v match {
      case a: JSArray  => a.value flatMap flattenTerms
      case _: JSObject => Seq(JSNull)
      case _           => Seq(v)
    }

    private def takeKnownObjects(atMost: Int) =
      knownObjects.take(Prop.int(atMost).sample + 1)

    @tailrec
    private def createWithConstraintViolationQuery(): FaunaArrayQuery =
      knownObjects.take(1).headOption match {
        case None =>
          createObjectsQuery() // no known objects available, fall back to a different operation
        case Some(known) =>
          val data = known / "data"
          val isUnique =
            !(data / "p1").isEmpty || !(data / "p2").isEmpty || !((data / "p3").isEmpty || (data / "p4").isEmpty)
          if (!isUnique) {
            createWithConstraintViolationQuery()
          } else {
            val auth = schema.serverKey.get

            val creates = sampleCreationQueries(auth)
            // Insert a duplicate somewhere in the sequence
            val duplicate = DBResource(
              auth,
              TestData.class1Ref,
              JSObject("data" -> data)
            ).toRequest.query
            val query = JSArray(insertAtRandomPos(creates, duplicate): _*)

            new FaunaArrayQuery(auth, query) {
              // Not strictly necessary, but we can use this object again
              override def errorResult(code: Int, body: JSObject) = {
                countOperations(Operation.UCVOnCreate, 1)
                knownObjects.add(Some(known))
                None
              }

              override def resultArray(arr: JSArray) = {
                log.warn(
                  s"Creation request\n${this.query}\nwith a duplicate was supposed to fail but it succeeded with \n$arr"
                )
                None
              }
            }
          }
      }

    private def insertAtRandomPos[T](values: Seq[T], value: T) = {
      if (values.isEmpty) {
        Seq(value)
      } else {
        val (left, right) = values.splitAt(Prop.int(values.size).sample)
        (left :+ value) ++ right
      }
    }

    private def deleteWithConstraintViolationQuery() =
      knownDeletes.take(1).headOption match {
        case None =>
          deleteObjectsQuery() // no known deletes available, delete some objects instead
        case Some(alreadyDeletedRef) =>
          val auth = schema.serverKey.get

          val existingObjects = takeKnownObjects(tConfig.MaxDeletedObjects)
          val existingRefs = existingObjects map { _ / "ref" }
          val existingAndAlreadyDeletedRef =
            insertAtRandomPos(existingRefs, alreadyDeletedRef)
          val query = JSArray(existingAndAlreadyDeletedRef map { ref =>
            DeleteF(ref)
          }: _*)

          new FaunaArrayQuery(auth, query) {
            override def resultArray(arr: JSArray) = {
              log.warn(
                s"Delete request\n${this.query}\nwith already deleted ref $alreadyDeletedRef was supposed to fail but it succeeded with \n$arr"
              )
              None
            }

            override def errorResult(code: Int, body: JSObject) = {
              // This is expected to fail with 404 because it tried to delete an already deleted ref
              if (code != 404) {
                log.warn(
                  s"Expected 404 in response to\n${this.query}\nbut got\n$code\ninstead and response\n$body"
                )
                None
              } else {
                countOperations(Operation.UCVOnDelete, 1)
                Some(
                  dataVerifyingQuery(
                    auth,
                    "fail-delete verifying",
                    true,
                    Operation.Read,
                    existingRefs map { ref =>
                      Get(ref)
                    },
                    existingObjects map { _ / "data" }
                  )
                )
              }
            }
          }
      }

    private def updateObjectsQuery() = {
      val knowns = takeKnownObjects(tConfig.MaxCreatedObjects)
      if (knowns.isEmpty) {
        createObjectsQuery()
      } else {
        val auth = schema.serverKey.get
        val newDataSeq =
          sampleSomeCreationQueries(auth, knowns.size) map getDataFromCreateAsObject
        val knownZipNewData = knowns zip newDataSeq
        val updates = knownZipNewData map {
          case (known, newData) =>
            Update(known / "ref", Quote(JSObject("data" -> newData)))
        }
        val expected = knownZipNewData map {
          case (known, newData) =>
            val knownData = (known / "data").asInstanceOf[JSObject]
            knownData.patch(newData)
        }
        dataVerifyingQuery(
          auth,
          "update",
          false,
          Operation.Update,
          updates,
          expected
        )
      }
    }

    private def readObjectsQuery() = {
      val knowns = takeKnownObjects(tConfig.MaxCreatedObjects)
      if (knowns.isEmpty) {
        createObjectsQuery()
      } else {
        dataVerifyingQuery(
          schema.serverKey.get,
          "read",
          true,
          Operation.Read,
          knowns map { obj =>
            Get(obj / "ref")
          },
          knowns map { _ / "data" }
        )
      }
    }

    private def verifyIndexesQuery() = {
      val knowns = takeKnownObjects(tConfig.MaxCreatedObjects)
      if (knowns.isEmpty) {
        createObjectsQuery()
      } else {
        makeIndexVerificationQueries(
          schema.serverKey.get,
          knowns,
          true,
          "weren't updated when objects were created or updated"
        )
      }
    }
  }
}

class Correctness(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new CorrectnessGenerator("Correctness", config)
  )
}
