package fauna.repo.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.doc._
import fauna.storage._
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._

class IndexerSpec extends Spec {

  val ctx = CassandraHelper.context("repo")

  def newScope = ScopeID(ctx.nextID())

  val faunaClass = CollectionID(1024)

  def ID(subID: Long) = DocID(SubID(subID), faunaClass)
  def TS(ts: Long) = Timestamp.ofMicros(ts)

  "Indexers" - {
    val scope = newScope

    val NameField = Field[String]("name")
    val EmailField = Field[String]("email")
    val FriendsField = Field[Vector[String]]("friends")
    val AddressField = Field[String]("addresses", "address")
    val FirstNameField = Field[String]("fullname", "first")
    val LastNameField = Field[String]("fullname", "last")

    val Index = IndexID(1)
    def ic(
      terms: Vector[(fauna.repo.TermExtractor, Boolean)],
      values: Vector[(fauna.repo.TermExtractor, Boolean)] = Vector.empty) =
      IndexConfig(scope, Index, faunaClass, terms, values)

    val NameIndex = ic(Vector(DefaultExtractor(NameField.path)))
    val FullnameIndex = ic(Vector(DefaultExtractor(List("fullname"))))
    val EmailIndex = ic(Vector.empty, Vector(DefaultExtractor(EmailField.path)))
    val EmptyIndex = ic(Vector.empty)
    val FriendsIndex = ic(Vector(DefaultExtractor(FriendsField.path)))
    val AddressIndex = ic(Vector(DefaultExtractor(AddressField.path)))
    val CompoundIndex =
      ic(Vector(DefaultExtractor(NameField.path), DefaultExtractor(EmailField.path)))
    val CoveredIndex = ic(
      Vector(DefaultExtractor(NameField.path)),
      Vector(DefaultExtractor(EmailField.path)))
    val VirtualIndex = ic(
      Vector(
        DefaultExtractor(List("class")),
        DefaultExtractor(List("ref")),
        DefaultExtractor(List("ts"))))

    "adds a field" in {
      val indexer = FieldIndexer(NameIndex)
      val v =
        Version.Live(
          scope,
          ID(1),
          AtValid(TS(1)),
          Create,
          SchemaVersion.Min,
          Data(NameField -> "fred"))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(
        scope,
        Index,
        Vector(IndexTerm(StringV("fred"))))
      rows.head.value.event shouldBe SetEvent(TS(1), scope, ID(1), Add)
    }

    "adds empty terms" in {
      val indexer = FieldIndexer(EmptyIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data.empty)

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(scope, Index, Vector.empty)
      rows.head.value.event shouldBe SetEvent(TS(1), scope, ID(1), Add)
    }

    "adds a virtual field" in {
      val indexer = FieldIndexer(VirtualIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data.empty)

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(
        scope,
        Index,
        Vector(
          IndexTerm(DocIDV(faunaClass.toDocID)),
          IndexTerm(DocIDV(ID(1))),
          IndexTerm(LongV(1))))
      rows.head.value.event shouldBe SetEvent(TS(1), scope, ID(1), Add)
    }

    "changes a field" in {
      val indexer = FieldIndexer(NameIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Update,
        SchemaVersion.Min,
        Data(NameField -> "waldo"),
        Some(Diff(NameField -> "fred")))

      val rows = (ctx ! indexer.rows(v)) map { i =>
        (i.key, i.value.event)
      }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("waldo")))),
          SetEvent(TS(2), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "changes a virtual field" in {
      val indexer = FieldIndexer(VirtualIndex)

      val diff = Diff(MapV("ts" -> LongV(1)))
      val v =
        Version.Live(
          scope,
          ID(1),
          AtValid(TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i =>
        (i.key, i.value.event)
      }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(DocIDV(faunaClass.toDocID)),
              IndexTerm(DocIDV(ID(1))),
              IndexTerm(LongV(2)))),
          SetEvent(TS(2), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(DocIDV(faunaClass.toDocID)),
              IndexTerm(DocIDV(ID(1))),
              IndexTerm(LongV(1)))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "changes valid time" in {
      val indexer = FieldIndexer(VirtualIndex)

      val diff = Diff(MapV("ts" -> LongV(1)))
      val v = Version.Live(
        scope,
        ID(1),
        Resolved(TS(3), TS(2)),
        Update,
        SchemaVersion.Min,
        Data.empty,
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i =>
        (i.key, i.value.event)
      }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(DocIDV(faunaClass.toDocID)),
              IndexTerm(DocIDV(ID(1))),
              IndexTerm(LongV(3)))),
          SetEvent(Resolved(TS(3), TS(2)), scope, ID(1), Add)))

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(DocIDV(faunaClass.toDocID)),
              IndexTerm(DocIDV(ID(1))),
              IndexTerm(LongV(1)))),
          SetEvent(Resolved(TS(3), TS(2)), scope, ID(1), Remove)))
    }

    "removes a field" in {
      val indexer = FieldIndexer(NameIndex)

      val diff = Diff(MapV("ts" -> LongV(1), "name" -> StringV("fred")))
      val v =
        Version.Live(
          scope,
          ID(1),
          AtValid(TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          Some(diff))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(
        scope,
        Index,
        Vector(IndexTerm(StringV("fred"))))
      rows.head.value.event shouldBe SetEvent(TS(2), scope, ID(1), Remove)
    }

    "removes empty terms" in {
      val indexer = FieldIndexer(EmptyIndex)

      val diff = Diff(MapV("ts" -> LongV(1)))
      val v =
        Version.Deleted(scope, ID(1), AtValid(TS(2)), SchemaVersion.Min, Some(diff))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(scope, Index, Vector.empty)
      rows.head.value.event shouldBe SetEvent(TS(2), scope, ID(1), Remove)
    }

    "removes a virtual field" in {
      val indexer = FieldIndexer(VirtualIndex)

      val diff = Diff(MapV("ts" -> LongV(1)))
      val v =
        Version.Deleted(scope, ID(1), AtValid(TS(2)), SchemaVersion.Min, Some(diff))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(
        scope,
        Index,
        Vector(
          IndexTerm(DocIDV(faunaClass.toDocID)),
          IndexTerm(DocIDV(ID(1))),
          IndexTerm(LongV(1))))
      rows.head.value.event shouldBe SetEvent(TS(2), scope, ID(1), Remove)
    }

    "removes valid time" in {
      val indexer = FieldIndexer(VirtualIndex)

      val diff = Diff(MapV("ts" -> LongV(3)))
      val v = Version.Deleted(
        scope,
        ID(1),
        Resolved(TS(4), TS(2)),
        SchemaVersion.Min,
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i =>
        (i.key, i.value.event)
      }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(DocIDV(faunaClass.toDocID)),
              IndexTerm(DocIDV(ID(1))),
              IndexTerm(LongV(3)))),
          SetEvent(Resolved(TS(4), TS(2)), scope, ID(1), Remove)))
    }

    "elides null values" in {
      val indexer = FieldIndexer(NameIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data.empty)

      (ctx ! indexer.rows(v)).isEmpty should be(true)
    }

    "adds multi-value attributes" in {
      val indexer = FieldIndexer(FriendsIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(FriendsField -> Vector("fred", "waldo")))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("waldo")))),
          SetEvent(TS(1), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(TS(1), scope, ID(1), Add)))
    }

    "changes multi-value attributes" in {
      val indexer = FieldIndexer(FriendsIndex)

      val diff = Diff(
        MapV(
          "ts" -> LongV(2),
          "friends" -> ArrayV(StringV("fred"), StringV("waldo"))))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(FriendsField -> Vector("fred")),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("waldo")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "removes multi-value attributes" in {
      val indexer = FieldIndexer(FriendsIndex)

      val diff = Diff(
        MapV(
          "ts" -> LongV(2),
          "friends" -> ArrayV(StringV("fred"), StringV("waldo"))))
      val v =
        Version.Live(
          scope,
          ID(1),
          AtValid(TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("waldo")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "adds nested multi-value attributes" in {
      val indexer = FieldIndexer(AddressIndex)
      val data = MapV(
        "addresses" ->
          ArrayV(
            MapV("address" -> StringV("123 Main St.")),
            MapV("address" -> StringV("456 First St."))))

      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(data))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("123 Main St.")))),
          SetEvent(TS(1), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("456 First St.")))),
          SetEvent(TS(1), scope, ID(1), Add)))
    }

    "adds compound values" in {
      val indexer = FieldIndexer(CompoundIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "fred", EmailField -> "fred@example.com"))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(StringV("fred")),
              IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(1), scope, ID(1), Add)))
    }

    "changes compound values" in {
      val indexer = FieldIndexer(CompoundIndex)

      val diff = Diff(MapV("ts" -> LongV(2), "name" -> StringV("fred")))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "freddy", EmailField -> "fred@example.com"),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(StringV("freddy")),
              IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(2), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(StringV("fred")),
              IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "removes compound values" in {
      val indexer = FieldIndexer(CompoundIndex)

      val diff = Diff(
        MapV(
          "ts" -> LongV(1),
          "name" -> StringV("fred"),
          "email" -> StringV("fred@example.com")))
      val v =
        Version.Deleted(scope, ID(1), AtValid(TS(2)), SchemaVersion.Min, Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(
              IndexTerm(StringV("fred")),
              IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "adds null terms" in {
      val indexer = FieldIndexer(CompoundIndex)

      val diff = Diff(
        MapV(
          "ts" -> LongV(1),
          "name" -> NullV,
          "email" -> StringV("fred@example.com")))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "fred"),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain((
        IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")), IndexTerm(NullV))),
        SetEvent(TS(2), scope, ID(1), Add)))
      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(IndexTerm(NullV), IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "elides entirely null terms" in {
      val indexer = FieldIndexer(CompoundIndex)

      val diff = Diff(MapV("ts" -> 1, "email" -> StringV("fred@example.com")))
      val v =
        Version.Live(
          scope,
          ID(1),
          AtValid(TS(2)),
          Create,
          SchemaVersion.Min,
          Data.empty,
          Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(IndexTerm(NullV), IndexTerm(StringV("fred@example.com")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "elides objects" in {
      val indexer = FieldIndexer(FullnameIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(FirstNameField -> "fred", LastNameField -> "friendly"))

      val rows = (ctx ! indexer.rows(v))
      rows.isEmpty should be(true)
    }

    "adds covered values" in {
      val indexer = FieldIndexer(CoveredIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "fred", EmailField -> "fred@example.com"))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(
            TS(1),
            scope,
            ID(1),
            Add,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "adds empty terms with covered values" in {
      val indexer = FieldIndexer(EmailIndex)
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(1)),
        Create,
        SchemaVersion.Min,
        Data(EmailField -> "fred@example.com"))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 1
      rows.head.key shouldBe IndexKey(scope, Index, Vector.empty)
      rows.head.value.event shouldBe SetEvent(
        TS(1),
        scope,
        ID(1),
        Add,
        Vector(IndexTerm(StringV("fred@example.com"))))
    }

    "changes covered values" in {
      val indexer = FieldIndexer(CoveredIndex)

      val diff = Diff(MapV("ts" -> LongV(1), "email" -> "fred@example.com"))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "fred", EmailField -> "freddy@example.com"),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Add,
            Vector(IndexTerm(StringV("freddy@example.com"))))))
      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Remove,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "changes empty terms with covered values" in {
      val indexer = FieldIndexer(EmailIndex)

      val diff = Diff(MapV("ts" -> LongV(1), "email" -> StringV("fred@example.com")))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(EmailField -> "freddy@example.com"),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 2

      rows should contain(
        (
          IndexKey(scope, Index, Vector.empty),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Add,
            Vector(IndexTerm(StringV("freddy@example.com"))))))
      rows should contain(
        (
          IndexKey(scope, Index, Vector.empty),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Remove,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "elides changed objects" in {
      val indexer = FieldIndexer(FullnameIndex)

      val diff = Diff(MapV("ts" -> LongV(1), "fullname" -> NullV))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(MapV("fullname" -> MapV("first" -> StringV("fred")))),
        Some(diff))

      val rows = ctx ! indexer.rows(v)
      rows.isEmpty should be(true)
    }

    "removes covered values" in {
      val indexer = FieldIndexer(CoveredIndex)

      val diff = Diff(
        MapV(
          "ts" -> LongV(1),
          "name" -> StringV("fred"),
          "email" -> StringV("fred@example.com")))
      val v =
        Version.Deleted(scope, ID(1), AtValid(TS(2)), SchemaVersion.Min, Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Remove,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "removes empty terms with covered values" in {
      val indexer = FieldIndexer(EmailIndex)

      val diff = Diff(MapV("ts" -> LongV(1), "email" -> StringV("fred@example.com")))
      val v =
        Version.Deleted(scope, ID(1), AtValid(TS(2)), SchemaVersion.Min, Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(scope, Index, Vector.empty),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Remove,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "elides entirely null values" in {
      val indexer = FieldIndexer(
        IndexConfig(
          scope,
          Index,
          faunaClass,
          Vector(
            DefaultExtractor(NameField.path),
            DefaultExtractor(EmailField.path)),
          Vector(DefaultExtractor(EmailField.path))))

      val diff = Diff(
        MapV(
          "ts" -> LongV(1),
          "name" -> NullV,
          "email" -> StringV("fred@example.com")))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(NameField -> "fred"),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(
            scope,
            Index,
            Vector(IndexTerm(NullV), IndexTerm(StringV("fred@example.com")))),
          SetEvent(
            TS(2),
            scope,
            ID(1),
            Remove,
            Vector(IndexTerm(StringV("fred@example.com"))))))
    }

    "removes transformed fields" in {
      val indexer = FieldIndexer(NameIndex)
      val v2 =
        Version.Deleted(
          scope,
          ID(1),
          AtValid(TS(2)),
          SchemaVersion.Min,
          Some(Diff(NameField -> "fred")))

      val rows = (ctx ! indexer.rows(v2)) map { i => (i.key, i.value.event) }
      rows.size shouldBe 1

      rows should contain(
        (
          IndexKey(scope, Index, Vector(IndexTerm(StringV("fred")))),
          SetEvent(TS(2), scope, ID(1), Remove)))
    }

    "add ttl field" in {
      val indexer = FieldIndexer(EmptyIndex)

      val diff = Diff(MapV("ttl" -> NullV, "ts" -> LongV(1)))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(MapV("ttl" -> TimeV(TS(101)))),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => i.value }
      rows shouldBe Seq(
        IndexValue(IndexTuple(scope, ID(1)), AtValid(TS(2)), Remove),
        IndexValue(
          IndexTuple(scope, ID(1), ttl = Some(TS(101))),
          AtValid(TS(2)),
          Add)
      )
    }

    "update ttl field" in {
      val indexer = FieldIndexer(EmptyIndex)

      val diff = Diff(MapV("ttl" -> TimeV(TS(100)), "ts" -> LongV(1)))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(MapV("ttl" -> TimeV(TS(101)))),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => i.value }
      rows shouldBe Seq(
        IndexValue(
          IndexTuple(scope, ID(1), ttl = Some(TS(100))),
          AtValid(TS(2)),
          Remove),
        IndexValue(
          IndexTuple(scope, ID(1), ttl = Some(TS(101))),
          AtValid(TS(2)),
          Add)
      )
    }

    "update ttl field to same value" in {
      val indexer = FieldIndexer(EmptyIndex)

      val diff = Diff(MapV("ttl" -> TimeV(TS(100)), "ts" -> LongV(1)))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(MapV("ttl" -> TimeV(TS(100)))),
        Some(diff))

      val rows = ctx ! indexer.rows(v)
      rows.size shouldBe 0
    }

    "remove ttl field" in {
      val indexer = FieldIndexer(EmptyIndex)

      val diff = Diff(MapV("ttl" -> TimeV(TS(100)), "ts" -> LongV(1)))
      val v = Version.Live(
        scope,
        ID(1),
        AtValid(TS(2)),
        Create,
        SchemaVersion.Min,
        Data(MapV("ttl" -> NullV)),
        Some(diff))

      val rows = (ctx ! indexer.rows(v)) map { i => i.value }
      rows shouldBe Seq(
        IndexValue(
          IndexTuple(scope, ID(1), ttl = Some(TS(100))),
          AtValid(TS(2)),
          Remove),
        IndexValue(IndexTuple(scope, ID(1)), AtValid(TS(2)), Add)
      )
    }
  }
}
