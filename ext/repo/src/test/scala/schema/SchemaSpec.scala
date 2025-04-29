package fauna.repo.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.prop._
import fauna.repo._
import fauna.repo.query.Query
import fauna.repo.schema._
import fauna.repo.schema.migration._
import fauna.repo.schema.ConstraintFailure.{ InvalidField, ReadOnlyFieldUpdate }
import fauna.repo.schema.FieldSchema._
import fauna.repo.WriteFailure.SchemaConstraintViolation
import fauna.snowflake.IDSource.{ Epoch => SnowflakeEpoch }
import fauna.storage.api.set._
import fauna.storage.doc.{ Data, Diff, Field }
import fauna.storage.ir._
import org.scalactic.source.Position
import scala.collection.immutable.{ SeqMap, SortedMap }

class CollectionSchemaSpec extends PropSpec {
  val repo = CassandraHelper.context("repo")

  def newScope = Prop.const(ScopeID(repo.nextID()))

  def allDocuments(coll: CollectionSchema): PagedQuery[Iterable[DocID]] = {
    val idx = coll.documentsIndex
    val term = coll.documentsIndexTerm map { t => Scalar(t.value) }
    val pq = Store.collection(idx, term, Timestamp.MaxMicros)
    pq mapValuesT { _.docID }
  }

  def defaultIndexes(scope: ScopeID): List[IndexConfig] =
    List(
      IndexConfig.DocumentsByCollection(scope),
      IndexConfig.ChangesByCollection(scope))

  def userCollSchema(
    scope: ScopeID,
    name: String,
    fields: Map[String, FieldSchema],
    id: CollectionID = UserCollectionID.MinValue,
    wildcard: Option[SchemaType] = None,
    indexes: List[IndexConfig] = Nil,
    idSource: DocIDSource = DocIDSource.Snowflake) =
    CollectionSchema.UserCollectionSchema(
      scope,
      id,
      name,
      MigrationList.empty,
      fields,
      wildcard = wildcard,
      indexes = indexes,
      idSource = idSource)

  test("basic CRUD") {
    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(ScalarType.Str))
    val coll = userCollSchema(scope, "Foo", fields, indexes = defaultIndexes(scope))

    val doc = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    val allDocs = repo ! allDocuments(coll).flattenT

    doc.data.fields
      .get(List("data", "a"))
      .get
      .asInstanceOf[StringV]
      .value shouldEqual "foo"
    allDocs shouldEqual Vector(doc.id)

    val doc2 = repo ! Store.replace(
      coll,
      doc.id,
      DataMode.Default,
      Data(Field[String]("a") -> "bar"))
    val allDocs2 = repo ! allDocuments(coll).flattenT

    doc2.id shouldEqual doc.id
    doc2.data(Field[String]("data", "a")) shouldEqual "bar"
    allDocs2 shouldEqual Vector(doc.id)

    // noop pass-through using updater variant
    val doc3 = repo ! Store.replace(
      coll,
      doc.id,
      DataMode.Default,
      Data(doc2.data.fields.get(List("data")).get.asInstanceOf[MapV]))
    val allDocs3 = repo ! allDocuments(coll).flattenT

    doc3.id shouldEqual doc.id
    doc3.data(Field[String]("data", "a")) shouldEqual "bar"
    allDocs3 shouldEqual Vector(doc.id)

    val doc4 = repo ! Store.replace(
      coll,
      doc.id,
      DataMode.Default,
      Data(Field[String]("a") -> "qux"))
    val allDocs4 = repo ! allDocuments(coll).flattenT

    doc4.id shouldEqual doc.id
    doc4.data(Field[String]("data", "a")) shouldEqual "qux"
    allDocs4 shouldEqual Vector(doc.id)

    val doc5 = repo ! Store.externalDelete(coll, doc.id)
    val allDocs5 = repo ! allDocuments(coll).flattenT

    doc5.id shouldEqual doc.id
    allDocs5 shouldEqual Vector.empty
  }

  test("changes collection works") {
    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(ScalarType.Str))
    val coll = userCollSchema(scope, "Foo", fields, indexes = defaultIndexes(scope))

    val doc = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    val idx = IndexConfig.ChangesByCollection(scope)
    val term = Vector(Scalar(coll.collID.toDocID))
    val pq = Store.collection(idx, term, Timestamp.MaxMicros)
    val allDocs = repo ! (pq mapValuesT { _.docID }).flattenT

    allDocs shouldEqual Vector(doc.id)
  }

  test("create with specified ID") {
    import WriteFailure.CreateDocIDExists

    val scope = newScope.sample
    val ids = DocIDSource.Sequential(1, 10)
    val coll = userCollSchema(
      scope,
      "Foo",
      SeqMap.empty,
      idSource = ids,
      indexes = defaultIndexes(scope))

    val doc1 = repo ! Store.create(coll, Data.empty)
    val allDocs1 = repo ! allDocuments(coll).flattenT

    doc1.id.subID.toLong shouldEqual 1
    allDocs1 shouldEqual Vector(doc1.id)

    val doc2 = repo ! Store.create(
      coll,
      DocID(SubID(2), coll.collID),
      DataMode.Default,
      Data.empty)
    val allDocs2 = repo ! allDocuments(coll).flattenT

    doc2.id.subID.toLong shouldEqual 2
    allDocs2 shouldEqual Vector(doc1.id, doc2.id)

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      doc1.id,
      DataMode.Default,
      Data.empty))
    ex3.failure shouldEqual CreateDocIDExists(scope, doc1.id)

    val doc4 = repo ! Store.create(coll, Data.empty)
    doc4.id.subID.toLong shouldEqual 3
  }

  test("ID sources") {
    val scope = newScope.sample
    val snowflake =
      userCollSchema(scope, "Snowflake", SeqMap.empty)
    val sequential = userCollSchema(
      scope,
      "Sequential",
      SeqMap.empty,
      id = CollectionID(UserCollectionID.MinValue.toLong + 1),
      idSource = DocIDSource.Sequential(1, 2))

    val doc1 = repo ! Store.create(snowflake, Data.empty)
    doc1.id.subID.toLong shouldBe >(SnowflakeEpoch)
    doc1.id.subID.toLong shouldBe <(Long.MaxValue)

    // Cannot specify IDs outside the bounds
    val invalidID1 = DocID(SubID(3), sequential.collID)
    val ex1 = the[WriteFailureException] thrownBy (repo ! Store.create(
      sequential,
      invalidID1,
      DataMode.Default,
      Data.empty))
    ex1.failure shouldEqual WriteFailure.InvalidID(scope, invalidID1)

    val invalidID2 = DocID(SubID(0), sequential.collID)
    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.create(
      sequential,
      invalidID2,
      DataMode.Default,
      Data.empty))
    ex2.failure shouldEqual WriteFailure.InvalidID(scope, invalidID2)

    val doc2 = repo ! Store.create(sequential, Data.empty)
    doc2.id.subID.toLong shouldEqual 1

    val doc3 = repo ! Store.create(sequential, Data.empty)
    doc3.id.subID.toLong shouldEqual 2

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      sequential,
      Data.empty))
    ex3.failure shouldEqual WriteFailure.MaxIDExceeded(scope, sequential.collID)
  }

  test("simple scalar field validation") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(ScalarType.Str))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a") -> "bar"))
    doc2.data(Field[String]("data", "a")) shouldEqual "bar"

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 2)))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            // FIXME: we need to translate this to what the user provided
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("a")),
                SchemaType.Scalar(ScalarType.Str),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }

    val ex4 =
      the[WriteFailureException] thrownBy (repo ! Store.create(coll, Data.empty))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            // FIXME: we need to translate this to what the user provided
            Seq(MissingField(Path(Right("data"), Right("a")), exp, "a")))
          if exp == SchemaType.Scalar(ScalarType.Str) =>
    }
  }

  test("immutable scalar field validation") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(ScalarType.Str, immutable = true))
    val coll = userCollSchema(scope, "Foo", fields)

    val expectedRec = SchemaType.ObjectType(fields.toMap)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))

    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a") -> "bar")))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(ImmutableFieldUpdate(Path(Right("data"), Right("a")), rec, "a"))
          ) if rec == expectedRec =>
    }

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data.empty))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(ImmutableFieldUpdate(Path(Right("data"), Right("a")), rec, "a")))
          if rec == expectedRec =>
    }

    val doc4 = repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a") -> "foo"))
    doc4.data(Field[String]("data", "a")) shouldEqual "foo"
  }

  test("readonly field validation") {
    val scope = newScope.sample
    val fields = SortedMap(
      "a" -> FieldSchema(SchemaType.Optional(ScalarType.Str), readOnly = true),
      "b" -> FieldSchema(SchemaType.Optional(ScalarType.Str)))
    val coll = userCollSchema(scope, "Foo", fields)

    val expectedRec = SchemaType.ObjectType(fields.toMap)

    // fail on create
    val ex1 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("a") -> "foo", Field[String]("b") -> "foo")))
    ex1.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(ReadOnlyFieldUpdate(Path(Right("data"), Right("a")), s, "a")))
          if s == expectedRec =>
    }

    val doc1 = repo ! Store.create(coll, Data(Field[String]("b") -> "foo"))

    // fail on customer update
    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.externalUpdate(
      coll,
      doc1.id,
      DataMode.Default,
      Diff(Field[String]("a") -> "foo")))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(ReadOnlyFieldUpdate(Path(Right("data"), Right("a")), s, "a")))
          if s == expectedRec =>
    }

    // fail on replace
    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a") -> "foo")))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(ReadOnlyFieldUpdate(Path(Right("data"), Right("a")), s, "a")))
          if s == expectedRec =>
    }

    // succeed on internal update
    val doc2 =
      repo ! Store.internalUpdate(coll, doc1.id, Diff(Field[String]("a") -> "bar"))
    doc2.data(Field[String]("data", "a")) shouldEqual "bar"
  }
  test("internal field validation") {
    val scope = newScope.sample
    val fields = SortedMap(
      "a" -> FieldSchema(SchemaType.Optional(ScalarType.Str), internal = true),
      "b" -> FieldSchema(SchemaType.Optional(ScalarType.Str)))
    val coll = userCollSchema(scope, "Foo", fields)

    val expectedRec = SchemaType.ObjectType(fields.toMap)

    // fail on create
    val ex1 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("a") -> "foo", Field[String]("b") -> "foo")))
    ex1.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(InvalidField(Path(Right("data"), Right("a")), exp, prov, "a")))
          if exp == expectedRec && prov == SchemaType.Record(
            "a" -> ScalarType.Str,
            "b" -> ScalarType.Str
          ) =>
    }

    val doc1 = repo ! Store.create(coll, Data(Field[String]("b") -> "foo"))

    // fail on customer update
    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.externalUpdate(
      coll,
      doc1.id,
      DataMode.Default,
      Diff(Field[String]("a") -> "foo")))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(InvalidField(Path(Right("data"), Right("a")), exp, prov, "a")))
          if exp == expectedRec && prov == SchemaType.Record(
            "a" -> ScalarType.Str,
            "b" -> ScalarType.Str
          ) =>
    }

    // fail on replace
    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a") -> "foo")))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(InvalidField(Path(Right("data"), Right("a")), exp, prov, "a")))
          if exp == expectedRec && prov == SchemaType.Record(
            "a" -> ScalarType.Str) =>
    }

    // succeed on internal update
    val doc2 =
      repo ! Store.internalUpdate(coll, doc1.id, Diff(Field[String]("a") -> "bar"))
    doc2.data(Field[String]("data", "a")) shouldEqual "bar"
  }

  test("custom field validation") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val customValidator: FieldValidator = { (pathPrefix, newVal, _) =>
      if (newVal == StringV("test")) {
        Query.value(
          Seq(
            InvalidField(pathPrefix.toPath, ScalarType.Str, ScalarType.Str, "TEST")))
      } else {
        Query.value(Nil)
      }
    }

    val scope = newScope.sample
    val fields =
      SortedMap("a" -> FieldSchema(ScalarType.Str, validator = customValidator))
    val coll = userCollSchema(scope, "Foo", fields)

    val ex1 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("a") -> "test")))
    ex1.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              InvalidField(
                Path(Right("data"), Right("a")),
                SchemaType.Scalar(ScalarType.Str),
                SchemaType.Scalar(ScalarType.Str),
                "TEST"))) =>
    }

    val doc2 = repo ! Store.create(coll, Data(Field[String]("a") -> "test2"))
    doc2.data(Field[String]("data", "a")) shouldEqual "test2"
  }

  test("singletons") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(ScalarType.Singleton("foo")))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("a") -> "bar")))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(Path(Right("data"), Right("a")), exp, prov)))
          if exp == SchemaType.Scalar(ScalarType.Singleton("foo"))
            && prov == SchemaType.Scalar(ScalarType.Singleton("bar")) =>
    }
  }

  test("unions: unions of singletons") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields = SortedMap(
      "a" -> FieldSchema(
        SchemaType.Union(ScalarType.Singleton("foo"), ScalarType.Singleton("bar"))))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.create(coll, Data(Field[String]("a") -> "bar"))
    doc2.data(Field[String]("data", "a")) shouldEqual "bar"

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("a") -> "baz")))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(Path(Right("data"), Right("a")), exp, prov)))
          if exp == SchemaType.Union(
            ScalarType.Singleton("foo"),
            ScalarType.Singleton("bar"))
            && prov == SchemaType.Scalar(ScalarType.Singleton("baz")) =>
    }

    val ex4 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 1)))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(Path(Right("data"), Right("a")), exp, prov)))
          if exp == SchemaType.Union(
            ScalarType.Singleton("foo"),
            ScalarType.Singleton("bar")) &&
            prov == SchemaType.Scalar(ScalarType.Int) =>
    }
  }

  test("unions: allow union of scalars") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields =
      SortedMap("a" -> FieldSchema(SchemaType.Union(ScalarType.Str, ScalarType.Int)))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.create(coll, Data(Field[Long]("a") -> 2))
    doc2.data(Field[Long]("data", "a")) shouldEqual 2

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data.empty))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(MissingField(Path(Right("data"), Right("a")), exp, "a")))
          if exp == SchemaType.Union(ScalarType.Str, ScalarType.Int) =>
    }
  }

  test("unions: nullable type (<type> | null) makes field optional") {
    val scope = newScope.sample
    val fields =
      SortedMap(
        "a" -> FieldSchema(SchemaType.Union(ScalarType.Str, ScalarType.Null)))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.replace(coll, doc1.id, DataMode.Default, Data.empty)
    doc2.data.fields.get(List("data")).get shouldEqual MapV.empty

    val doc3 = repo ! Store.replace(
      coll,
      doc1.id,
      DataMode.Default,
      Data(MapV(List("a" -> NullV))))
    doc3.data.fields.get(List("data")).get shouldEqual MapV.empty

    val doc4 = repo ! Store.create(coll, Data(MapV(List("a" -> NullV))))
    doc4.data.fields.get(List("data")).get shouldEqual MapV.empty

    val doc5 = repo ! Store.create(coll, Data.empty)
    doc5.data(Field[Option[String]]("data", "a")) shouldEqual None
  }

  test("unions: unions of structs") {
    import ConstraintFailure._
    import SchemaType._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields =
      SeqMap(
        "x" -> FieldSchema(
          SchemaType.Union(
            Record("a" -> FieldSchema(ScalarType.Str)),
            Record("b" -> FieldSchema(ScalarType.Int)))))
    val coll = userCollSchema(scope, "Qux", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("x", "a") -> "foo"))
    doc1.data(Field[String]("data", "x", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.create(coll, Data(Field[Long]("x", "b") -> 2))
    doc2.data(Field[Long]("data", "x", "b")) shouldEqual 2

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("x", "a") -> 2)))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("x"), Right("a")),
                SchemaType.Scalar(ScalarType.Str),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }

    val ex4 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("x", "b") -> "foo")))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("x"), Right("b")),
                SchemaType.Scalar(ScalarType.Int),
                SchemaType.Scalar(ScalarType.Str)))) =>
    }

    val ex5 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Boolean]("x", "c") -> false)))
    ex5.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              InvalidField(Path(Right("data"), Right("x"), Right("c")), _, _, "c"),
              UnionMissingFields(Path(Right("data"), Right("x")), _))
          ) =>
    }
  }

  test("unions: unions of structs with scalars") {
    import ConstraintFailure._
    import SchemaType._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields =
      SortedMap(
        "x" -> FieldSchema(SchemaType
          .Union(Record("a" -> FieldSchema(ScalarType.Str)), ScalarType.Str)))
    val coll = userCollSchema(scope, "Qux", fields)

    val doc1 = repo ! Store.create(coll, Data(Field[String]("x", "a") -> "foo"))
    doc1.data(Field[String]("data", "x", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.create(coll, Data(Field[String]("x") -> "foo"))
    doc2.data(Field[String]("data", "x")) shouldEqual "foo"

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("x", "a") -> 2)))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("x"), Right("a")),
                SchemaType.Scalar(ScalarType.Str),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }

    val ex4 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[String]("x", "b") -> "foo")))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              MissingField(Path(Right("data"), Right("x"), Right("a")), _, "a"))) =>
    }

    // If current code can't generate a nice error, report a mismatch against the
    // union.

    val ex5 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Boolean]("x") -> false)))
    ex5.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("x")),
                exp,
                SchemaType.Scalar(ScalarType.Boolean))))
          if exp == SchemaType.Union(
            SchemaType.Record("a" -> ScalarType.Str),
            ScalarType.Str) =>
    }
  }

  test("struct field validation") {
    import ConstraintFailure._
    import SchemaType._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val foo = userCollSchema(
      scope,
      "Foo",
      SortedMap(
        "a" -> FieldSchema(
          Record("b" -> FieldSchema(ScalarType.Str, immutable = true)))))

    val doc1 = repo ! Store.create(foo, Data(Field[String]("a", "b") -> "foo"))
    doc1.data(Field[String]("data", "a", "b")) shouldEqual "foo"

    val ex2 =
      the[WriteFailureException] thrownBy (repo ! Store.create(foo, Data.empty))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(MissingField(Path(Right("data"), Right("a"), Right("b")), exp, "b")))
          if exp == SchemaType.Scalar(ScalarType.Str) =>
    }

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      foo,
      Data(MapV(List("a" -> MapV.empty)))))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(MissingField(Path(Right("data"), Right("a"), Right("b")), exp, "b")))
          if exp == SchemaType.Scalar(ScalarType.Str) =>
    }

    //    Data(doc2.data.fields.get(List("data")).get.asInstanceOf[MapV]))
    val doc4 = repo ! Store.replace(
      foo,
      doc1.id,
      DataMode.Default,
      Data(doc1.data.fields.get(List("data")).get.asInstanceOf[MapV]))
    doc4.data shouldEqual doc1.data

    val ex5 = the[WriteFailureException] thrownBy (repo ! Store.replace(
      foo,
      doc1.id,
      DataMode.Default,
      Data(Field[String]("a", "b") -> "bar")))
    ex5.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              ImmutableFieldUpdate(
                Path(Right("data"), Right("a"), Right("b")),
                n,
                "b")))
          if n == SchemaType.Record(
            "b" -> FieldSchema(ScalarType.Str, immutable = true)) =>
    }
  }

  test("structs: immutable structs") { pending }
  test("structs: optional structs") { pending }
  test("structs: optional members") { pending }

  test("doc refs") {
    pending

    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val foo = userCollSchema(scope, "Foo", SeqMap.empty, id = CollectionID(2000))
    val qux = userCollSchema(scope, "Qux", SeqMap.empty, id = CollectionID(2001))
    val bar = userCollSchema(
      scope,
      "Bar",
      SortedMap("foo" -> FieldSchema(ScalarType.DocType(CollectionID(2000), "Foo"))),
      id = CollectionID(2002))

    val foo1 = repo ! Store.create(foo, Data.empty)
    val qux1 = repo ! Store.create(qux, Data.empty)

    val doc1 = repo ! Store.create(bar, Data(Field[DocID]("foo") -> foo1.id))
    doc1.data(Field[DocID]("data", "foo")) shouldEqual foo1.id

    val ex2 = the[WriteFailureException] thrownBy (repo ! Store.create(
      bar,
      Data(Field[Long]("foo") -> 2)))
    ex2.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("foo")),
                SchemaType.Scalar(ScalarType.DocType(CollectionID(2000), "Foo")),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      bar,
      Data(Field[DocID]("foo") -> qux1.id)))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(
              Path(Right("data"), Right("foo")),
              SchemaType.Scalar(ScalarType.DocType(CollectionID(2000), "Foo")),
              SchemaType.Scalar(ScalarType.DocType(CollectionID(2001), "Qux"))
            ))) =>
    }
  }

  test("arrays") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields = SortedMap("a" -> FieldSchema(SchemaType.Array(ScalarType.Str)))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector("foo")))
    doc1.data(Field[Vector[String]]("data", "a")) shouldEqual Vector("foo")

    val doc2 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector.empty))
    doc2.data(Field[Vector[String]]("data", "a")) shouldEqual Vector.empty

    val ex3 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 2)))
    ex3.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("a")),
                SchemaType.Array(SchemaType.Scalar(ScalarType.Str)),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }
  }

  test("arrays: optional arrays") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields =
      SortedMap(
        "a" -> FieldSchema(
          SchemaType.Union(SchemaType.Array(ScalarType.Str), ScalarType.Null)))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector("foo")))
    doc1.data(Field[Vector[String]]("data", "a")) shouldEqual Vector("foo")

    val doc2 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector.empty))
    doc2.data(Field[Vector[String]]("data", "a")) shouldEqual Vector.empty

    val doc3 = repo ! Store.create(coll, Data.empty)
    doc3.data.fields.get(List("data")).get shouldEqual MapV.empty

    val ex4 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 2)))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(Path(Right("data"), Right("a")), exp, prov)))
          if exp == SchemaType.Union(
            SchemaType.Array(ScalarType.Str),
            ScalarType.Null)
            && prov == SchemaType.Scalar(ScalarType.Int) =>
    }
  }

  test("arrays: nullable array elems") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val fields =
      SortedMap(
        "a" -> FieldSchema(
          SchemaType.Array(SchemaType.Union(ScalarType.Str, ScalarType.Null))))
    val coll = userCollSchema(scope, "Foo", fields)

    val doc1 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector("foo")))
    doc1.data(Field[Vector[String]]("data", "a")) shouldEqual Vector("foo")

    val doc2 =
      repo ! Store.create(coll, Data(Field[Vector[String]]("a") -> Vector.empty))
    doc2.data(Field[Vector[String]]("data", "a")) shouldEqual Vector.empty

    val doc3 = repo ! Store.create(
      coll,
      Data(Field[Vector[ScalarV]]("a") -> Vector(StringV("foo"), NullV)))
    doc3.data(Field[Vector[ScalarV]]("data", "a")) shouldEqual Vector(
      StringV("foo"),
      NullV)

    val doc4 =
      repo ! Store.create(coll, Data(Field[Vector[ScalarV]]("a") -> Vector(NullV)))
    doc4.data(Field[Vector[ScalarV]]("data", "a")) shouldEqual Vector(NullV)

    val ex5 =
      the[WriteFailureException] thrownBy (repo ! Store.create(coll, Data.empty))
    ex5.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(MissingField(Path(Right("data"), Right("a")), exp, "a")))
          if exp == SchemaType.Array(
            SchemaType.Union(ScalarType.Str, ScalarType.Null)) =>
    }

    val ex6 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 2)))
    ex6.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(TypeMismatch(Path(Right("data"), Right("a")), exp, prov)))
          if exp == SchemaType.Array(SchemaType
            .Union(ScalarType.Str, ScalarType.Null)) && prov == SchemaType.Scalar(
            ScalarType.Int) =>
    }
  }

  test("arrays: immutable arrays") { pending }
  test("arrays: immutable struct field in arrays") { pending }
  test("arrays: optional struct field in arrays") { pending }

  test("wildcards") {
    import ConstraintFailure._
    import WriteFailure.SchemaConstraintViolation

    val scope = newScope.sample
    val coll =
      userCollSchema(scope, "Foo", SortedMap.empty, wildcard = Some(ScalarType.Str))

    val doc1 = repo ! Store.create(coll, Data(Field[String]("a") -> "foo"))
    doc1.data(Field[String]("data", "a")) shouldEqual "foo"

    val doc2 = repo ! Store.create(coll, Data(Field[String]("b") -> "foo"))
    doc2.data(Field[String]("data", "b")) shouldEqual "foo"

    val doc3 = repo ! Store.create(coll, Data.empty)
    doc3.data.fields.get(List("data")).get shouldEqual MapV.empty

    val ex4 = the[WriteFailureException] thrownBy (repo ! Store.create(
      coll,
      Data(Field[Long]("a") -> 2)))
    ex4.failure should matchPattern {
      case SchemaConstraintViolation(
            Seq(
              TypeMismatch(
                Path(Right("data"), Right("a")),
                SchemaType.Scalar(ScalarType.Str),
                SchemaType.Scalar(ScalarType.Int)))) =>
    }
  }

  // Migration cases. Advanced cases may need to move to ext/model

  test("migration: safe field rename") { pending }
  test("migration: safe field removal") { pending }

  test("migration: safe optional field add") { pending }
  test("migration: safe field add with default") { pending }
  test("migration: safe field modification with conversion function") { pending }

  // Unsafe migrations should be allowed with a "force" flag on schema update.

  // It is unsafe to migrate a field to a more restricted type, or specifically not a
  // super-type
  test("migration: unsafe optional -> required modification") { pending }
  test(
    "migration: unsafe union reduction (generalization of optional -> required)") {
    pending
  }
  test("migration: unsafe field add (null -> non-nullable type)") { pending }

  val scope = newScope.sample
  def makeSchema(fields: (String, FieldSchema)*): CollectionSchema =
    userCollSchema(scope, "Foo", fields.to(SeqMap))

  def checkOk(schema: CollectionSchema, data: Data) = {
    val doc = repo ! Store.create(schema, data)
    doc.data.fields.get(List("data")).get
  }

  def checkErr(schema: CollectionSchema, data: Data, expected: Seq[String])(
    implicit pos: Position): Unit = {
    try {
      repo ! Store.create(schema, data)
      fail(s"$data worked against $schema")
    } catch {
      case e: WriteFailureException =>
        e.failure
          .asInstanceOf[SchemaConstraintViolation]
          .failures
          .map(f => s"${f.label}: ${f.message}") shouldEqual expected
    }
  }

  test("constraint failure rendering") {
    val schema0 = makeSchema("foo" -> FieldSchema(ScalarType.Str))
    checkErr(
      schema0,
      Data.empty,
      Seq("data.foo: Missing required field of type String"))
    checkOk(schema0, Data(Field[String]("foo") -> "hello"))
    checkErr(
      schema0,
      Data(Field[Long]("foo") -> 3),
      Seq("data.foo: Expected String, provided Int"))

    val schema1 = makeSchema("foo" -> SchemaType.Optional(ScalarType.Str))
    checkOk(schema1, Data.empty)
    checkOk(schema1, Data(Field[String]("foo") -> "hello"))
    checkErr(
      schema1,
      Data(Field[Long]("foo") -> 3),
      Seq("data.foo: Expected String | Null, provided Int"))

    val schema2 = makeSchema(
      "foo" -> FieldSchema(
        SchemaType
          .Union(
            SchemaType.Record("a" -> FieldSchema(ScalarType.Str)),
            SchemaType.Record("b" -> FieldSchema(ScalarType.Str))
          )))
    checkOk(schema2, Data(MapV("foo" -> MapV("a" -> "hello"))))
    checkOk(schema2, Data(MapV("foo" -> MapV("b" -> "hello"))))
    checkErr(
      schema2,
      Data(MapV("foo" -> MapV("c" -> "hello"))),
      Seq(
        "data.foo.c: Unexpected field provided",
        "data.foo: Union requires fields [a] or [b]"
      ))

    val schema3 = makeSchema(
      "foo" -> FieldSchema(
        SchemaType
          .Union(
            SchemaType.Record("a" -> FieldSchema(ScalarType.Str)),
            SchemaType.Record("b" -> FieldSchema(ScalarType.Str)),
            SchemaType.Record("c" -> FieldSchema(ScalarType.Str))
          )))
    checkOk(schema3, Data(MapV("foo" -> MapV("a" -> "hello"))))
    checkOk(schema3, Data(MapV("foo" -> MapV("b" -> "hello"))))
    checkOk(schema3, Data(MapV("foo" -> MapV("c" -> "hello"))))
    checkErr(
      schema3,
      Data(MapV("foo" -> MapV("d" -> "hello"))),
      Seq(
        "data.foo.d: Unexpected field provided",
        "data.foo: Union requires fields [a], [b], or [c]"
      ))
  }

  test("ints") {
    val schema0 = makeSchema("foo" -> FieldSchema(ScalarType.Int))

    val long = scala.Int.MaxValue.toLong + 1

    checkOk(schema0, Data(MapV("foo" -> LongV(0))))
    checkErr(
      schema0,
      Data(MapV("foo" -> LongV(long))),
      Seq(
        "data.foo: Expected Int, provided Long"
      ))
    checkErr(
      schema0,
      Data(MapV("foo" -> DoubleV(2.0))),
      Seq(
        "data.foo: Expected Int, provided Double"
      ))
  }

  test("longs") {
    val schema0 = makeSchema("foo" -> FieldSchema(ScalarType.Long))

    val long = scala.Int.MaxValue.toLong + 1

    checkOk(schema0, Data(MapV("foo" -> LongV(0))))
    checkOk(schema0, Data(MapV("foo" -> LongV(long))))
    checkErr(
      schema0,
      Data(MapV("foo" -> DoubleV(2.0))),
      Seq(
        "data.foo: Expected Long, provided Double"
      ))
  }

  test("doubles") {
    val schema0 = makeSchema("foo" -> FieldSchema(ScalarType.Double))

    val long = scala.Int.MaxValue.toLong + 1

    checkErr(
      schema0,
      Data(MapV("foo" -> LongV(0))),
      Seq(
        "data.foo: Expected Double, provided Int"
      ))
    checkErr(
      schema0,
      Data(MapV("foo" -> LongV(long))),
      Seq(
        "data.foo: Expected Double, provided Long"
      ))
    checkOk(schema0, Data(MapV("foo" -> DoubleV(2.0))))
  }

  test("numbers") {
    val schema0 = makeSchema("foo" -> ScalarType.Number)

    val long = scala.Int.MaxValue.toLong + 1

    checkOk(schema0, Data(MapV("foo" -> LongV(0))))
    checkOk(schema0, Data(MapV("foo" -> LongV(long))))
    checkOk(schema0, Data(MapV("foo" -> DoubleV(2.0))))
  }

  test("any") {
    val schema0 = makeSchema("foo" -> ScalarType.Any)

    checkOk(schema0, Data(MapV("foo" -> 0)))
    checkOk(schema0, Data(MapV("foo" -> (scala.Int.MaxValue.toLong + 1))))
    checkOk(schema0, Data(MapV("foo" -> 2.0)))
    checkOk(schema0, Data(MapV("foo" -> "hello")))
    checkOk(schema0, Data(MapV("foo" -> NullV)))
  }
}
