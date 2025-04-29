package fauna.api.test

import fauna.atoms._
import fauna.lang.Timestamp
import fauna.repo.store.{ DatabaseTree, DumpEntry }
import java.io.ByteArrayInputStream
import org.scalatest.OptionValues

class DatabaseTreeSpec extends Spec with OptionValues {

  val root = DatabaseTree(
    parentScopeID = ScopeID.RootID,
    dbID = DatabaseID.RootID,
    scopeID = ScopeID.RootID,
    globalID = GlobalDatabaseID.MinValue,
    deletedTS = None
  )

  "DumpEntry" - {
    "parse works" in {
      val str = """
                  |{
                  |  "entries": [
                  |    {"type": "scope_id", "id": "1", "parent_id": "0", "db_id": "1"},
                  |    {"type": "global_id", "id": "yyyyyyyyyyyyn", "parent_id": "0", "db_id": "1"},
                  |
                  |    {"type": "scope_id", "id": "2", "parent_id": "0", "db_id": "2"},
                  |    {"type": "global_id", "id": "yyyyyyyyyyyyr", "parent_id": "0", "db_id": "2"},
                  |
                  |    {"type": "scope_id", "id": "3", "parent_id": "1", "db_id": "3"},
                  |    {"type": "global_id", "id": "yyyyyyyyyyyyg", "parent_id": "1", "db_id": "3"},
                  |
                  |    {"type": "scope_id", "id": "4", "parent_id": "2", "db_id": "4"},
                  |    {"type": "global_id", "id": "yyyyyyyyyyyye", "parent_id": "2", "db_id": "4"},
                  |
                  |    {"type": "scope_id", "id": "5", "parent_id": "0", "db_id": "5", "deleted_ts": "1970-01-01T00:00Z"},
                  |    {"type": "global_id", "id": "yyyyyyyyyyyyk", "parent_id": "0", "db_id": "5", "deleted_ts": "1970-01-01T00:00Z"}
                  |  ]
                  |}
                  |""".stripMargin

      val stream = new ByteArrayInputStream(str.getBytes)

      DumpEntry.fromStream(stream) shouldBe Seq(
        DumpEntry(ScopeID(1), ScopeID(0), DatabaseID(1), None),
        DumpEntry(GlobalDatabaseID(1), ScopeID(0), DatabaseID(1), None),
        DumpEntry(ScopeID(2), ScopeID(0), DatabaseID(2), None),
        DumpEntry(GlobalDatabaseID(2), ScopeID(0), DatabaseID(2), None),
        DumpEntry(ScopeID(3), ScopeID(1), DatabaseID(3), None),
        DumpEntry(GlobalDatabaseID(3), ScopeID(1), DatabaseID(3), None),
        DumpEntry(ScopeID(4), ScopeID(2), DatabaseID(4), None),
        DumpEntry(GlobalDatabaseID(4), ScopeID(2), DatabaseID(4), None),
        DumpEntry(ScopeID(5), ScopeID(0), DatabaseID(5), Some(Timestamp.Epoch)),
        DumpEntry(
          GlobalDatabaseID(5),
          ScopeID(0),
          DatabaseID(5),
          Some(Timestamp.Epoch))
      )
    }
  }

  "DatabaseTreeSpec" - {
    "works" in {
      // root = (s000, g000, id000)
      //   child1 = (s001, g001, id001)
      //     grand-child1 = (s003, g003, id003)
      //   child2 = (s002, g002, id002)
      //     grand-child2 = (s004, g004, id004)
      //   child3 = (s005, g005, id005) deleted at Timestamp.Epoch

      val entries = Seq(
        DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
        DumpEntry(
          GlobalDatabaseID.MinValue,
          ScopeID.RootID,
          DatabaseID.RootID,
          None),
        DumpEntry(ScopeID(1), ScopeID(0), DatabaseID(1), None),
        DumpEntry(GlobalDatabaseID(1), ScopeID(0), DatabaseID(1), None),
        DumpEntry(ScopeID(2), ScopeID(0), DatabaseID(2), None),
        DumpEntry(GlobalDatabaseID(2), ScopeID(0), DatabaseID(2), None),
        DumpEntry(ScopeID(3), ScopeID(1), DatabaseID(3), None),
        DumpEntry(GlobalDatabaseID(3), ScopeID(1), DatabaseID(3), None),
        DumpEntry(ScopeID(4), ScopeID(2), DatabaseID(4), None),
        DumpEntry(GlobalDatabaseID(4), ScopeID(2), DatabaseID(4), None),
        DumpEntry(ScopeID(5), ScopeID(0), DatabaseID(5), Some(Timestamp.Epoch)),
        DumpEntry(
          GlobalDatabaseID(5),
          ScopeID(0),
          DatabaseID(5),
          Some(Timestamp.Epoch))
      )

      val child1 = DatabaseTree(
        parentScopeID = ScopeID.RootID,
        dbID = DatabaseID(1),
        scopeID = ScopeID(1),
        globalID = GlobalDatabaseID(1),
        deletedTS = None
      )

      val grandChild1 = DatabaseTree(
        parentScopeID = child1.scopeID,
        dbID = DatabaseID(3),
        scopeID = ScopeID(3),
        globalID = GlobalDatabaseID(3),
        deletedTS = None
      )

      val child2 = DatabaseTree(
        parentScopeID = ScopeID.RootID,
        dbID = DatabaseID(2),
        scopeID = ScopeID(2),
        globalID = GlobalDatabaseID(2),
        deletedTS = None
      )

      val grandChild2 = DatabaseTree(
        parentScopeID = child2.scopeID,
        dbID = DatabaseID(4),
        scopeID = ScopeID(4),
        globalID = GlobalDatabaseID(4),
        deletedTS = None
      )

      val child3 = DatabaseTree(
        parentScopeID = ScopeID.RootID,
        dbID = DatabaseID(5),
        scopeID = ScopeID(5),
        globalID = GlobalDatabaseID(5),
        deletedTS = Some(Timestamp.Epoch)
      )

      val result = DatabaseTree.build(entries)

      result shouldBe root
      result.forGlobalID(GlobalDatabaseID(1)).value shouldBe child1
      result.forGlobalID(GlobalDatabaseID(2)).value shouldBe child2
      result.forGlobalID(GlobalDatabaseID(3)).value shouldBe grandChild1
      result.forGlobalID(GlobalDatabaseID(4)).value shouldBe grandChild2
      result.forGlobalID(GlobalDatabaseID(5)).value shouldBe child3
    }

    "cannot have more than one scope/global id mapping a database" in {
      // global id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(GlobalDatabaseID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(GlobalDatabaseID(4321L), ScopeID(1L), DatabaseID(1L), None)
        )

        DatabaseTree.build(entries)
      } should have message "There can be only one live global id entry mapping a database, found (yyyyyyyyyynpr, yyyyyyyyyyeqn)"

      // scope id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(ScopeID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(ScopeID(4321L), ScopeID(1L), DatabaseID(1L), None)
        )

        DatabaseTree.build(entries)
      } should have message "There can be only one live scope id entry mapping a database, found (1234, 4321)"

      // key id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(GlobalKeyID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(GlobalKeyID(4321L), ScopeID(1L), DatabaseID(1L), None)
        )

        DatabaseTree.build(entries)
      } should have message "GlobalKeyID is not supported, found (1234,4321)"
    }

    "one scope/global id cannot map different databases" in pendingUntilFixed {
      // global id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(GlobalDatabaseID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(GlobalDatabaseID(1234L), ScopeID(1L), DatabaseID(2L), None)
        )

        DatabaseTree.build(entries)
      } should have message "A live global ID can only map a single database, found yyyyyyyyyynpr => [(ScopeID(1),DatabaseID(1)), (ScopeID(1),DatabaseID(2))]"

      // scope id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(ScopeID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(ScopeID(1234L), ScopeID(1L), DatabaseID(2L), None)
        )

        DatabaseTree.build(entries)
      } should have message "A live scope ID can only map a single database, found 1234 => [(ScopeID(1),DatabaseID(1)), (ScopeID(1),DatabaseID(2))]"

      // key id
      the[Exception] thrownBy {
        val entries = List(
          // parents
          DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
          DumpEntry(ScopeID(1L), ScopeID.RootID, DatabaseID(1), None),

          // nodes
          DumpEntry(GlobalKeyID(1234L), ScopeID(1L), DatabaseID(1L), None),
          DumpEntry(GlobalKeyID(1234L), ScopeID(1L), DatabaseID(2L), None)
        )

        DatabaseTree.build(entries)
      } should have message "GlobalKeyID is not supported, found 1234"
    }

    "forGlobalID should return the latest entry" in {
      val entries = List(
        // parents
        DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
        DumpEntry(ScopeID(327758728885436928L), ScopeID.RootID, DatabaseID(1), None),
        DumpEntry(ScopeID(327758700191154688L), ScopeID.RootID, DatabaseID(2), None),
        DumpEntry(ScopeID(327758671898477056L), ScopeID.RootID, DatabaseID(3), None),
        DumpEntry(ScopeID(327758633461875200L), ScopeID.RootID, DatabaseID(4), None),
        DumpEntry(ScopeID(327758578257494528L), ScopeID.RootID, DatabaseID(5), None),

        // nodes
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758728885436928L),
          DatabaseID(327758578347672064L),
          Some(Timestamp.parse("2022-04-01T17:25:59.710Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758700191154688L),
          DatabaseID(327758578347672064L),
          Some(Timestamp.parse("2022-04-01T17:25:30.620Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758671898477056L),
          DatabaseID(327758578347672064L),
          Some(Timestamp.parse("2022-04-01T17:25:03.370Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758633461875200L),
          DatabaseID(327758578347672064L),
          Some(Timestamp.parse("2022-04-01T17:24:35.860Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758578257494528L),
          DatabaseID(327758758085132800L),
          None
        ), // alive entry
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758578257494528L),
          DatabaseID(327758727610368512L),
          Some(Timestamp.parse("2022-04-01T17:25:59.710Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758578257494528L),
          DatabaseID(327758699052401152L),
          Some(Timestamp.parse("2022-04-01T17:25:30.620Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758578257494528L),
          DatabaseID(327758670121140736L),
          Some(Timestamp.parse("2022-04-01T17:25:03.370Z"))),
        DumpEntry(
          GlobalDatabaseID(327758578348721664L),
          ScopeID(327758578257494528L),
          DatabaseID(327758578347672064L),
          Some(Timestamp.parse("2022-04-01T17:24:35.860Z")))
      )

      val tree = DatabaseTree.build(entries)

      tree.forGlobalID(GlobalDatabaseID(327758578348721664L)).value shouldBe
        DatabaseTree(
          ScopeID(327758578257494528L),
          DatabaseID(327758758085132800L),
          ScopeID.MaxValue,
          GlobalDatabaseID(327758578348721664L),
          None)
    }

    "default globalID to scopeID" in {
      val entries = Seq(
        DumpEntry(ScopeID.RootID, ScopeID.RootID, DatabaseID.RootID, None),
        DumpEntry(
          GlobalDatabaseID.MinValue,
          ScopeID.RootID,
          DatabaseID.RootID,
          None),
        DumpEntry(ScopeID(123), ScopeID(0), DatabaseID(1), None)
      )

      val child = DatabaseTree(
        parentScopeID = ScopeID.RootID,
        dbID = DatabaseID(1),
        scopeID = ScopeID(123),
        globalID = GlobalDatabaseID(123),
        deletedTS = None
      )

      val result = DatabaseTree.build(entries)

      result shouldBe root
      result.forGlobalID(GlobalDatabaseID(123)).value shouldBe child
    }

    "consider deleted entries" in {
      val entries = Seq(
        DumpEntry(ScopeID(0), ScopeID(0), DatabaseID(0), None),
        DumpEntry(GlobalDatabaseID(0), ScopeID(0), DatabaseID(0), None),
        DumpEntry(
          ScopeID(331576231828914272L),
          ScopeID(0),
          DatabaseID(331576231822622816L),
          Some(Timestamp.parse("2022-05-13T20:44:37.480Z"))),
        DumpEntry(
          GlobalDatabaseID(331576231829962848L),
          ScopeID(0),
          DatabaseID(331576231822622816L),
          Some(Timestamp.parse("2022-05-13T20:44:37.480Z"))),
        DumpEntry(
          ScopeID(297327065625002081L),
          ScopeID(0),
          DatabaseID(297327065616613473L),
          None),
        DumpEntry(
          GlobalDatabaseID(297327065625002081L),
          ScopeID(0),
          DatabaseID(297327065616613473L),
          None),
        DumpEntry(
          ScopeID(331576233007513696L),
          ScopeID(309104418431696993L),
          DatabaseID(331576326748110944L),
          None),
        DumpEntry(
          GlobalDatabaseID(331500773353457761L),
          ScopeID(309104418431696993L),
          DatabaseID(331576326748110944L),
          None),
        DumpEntry(
          ScopeID(331576233007513696L),
          ScopeID(331576231828914272L),
          DatabaseID(331500773353455713L),
          Some(Timestamp.parse("2022-05-13T20:44:36.850Z"))),
        DumpEntry(
          GlobalDatabaseID(331500773353457761L),
          ScopeID(331576231828914272L),
          DatabaseID(331500773353455713L),
          Some(Timestamp.parse("2022-05-13T20:44:36.850Z"))),
        DumpEntry(
          ScopeID(331576233007515744L),
          ScopeID(331576233007513696L),
          DatabaseID(331500788328169569L),
          None),
        DumpEntry(
          GlobalDatabaseID(331500788328171617L),
          ScopeID(331576233007513696L),
          DatabaseID(331500788328169569L),
          None),
        DumpEntry(
          ScopeID(331576233007514720L),
          ScopeID(331576233007513696L),
          DatabaseID(331500777953561697L),
          None),
        DumpEntry(
          GlobalDatabaseID(331500777953563745L),
          ScopeID(331576233007513696L),
          DatabaseID(331500777953561697L),
          None),
        DumpEntry(
          ScopeID(331576233007516768L),
          ScopeID(331576233007513696L),
          DatabaseID(331500778495672417L),
          None),
        DumpEntry(
          GlobalDatabaseID(331500778495674465L),
          ScopeID(331576233007513696L),
          DatabaseID(331500778495672417L),
          None),
        DumpEntry(
          ScopeID(331576233007517792L),
          ScopeID(331576233007513696L),
          DatabaseID(331500787813318753L),
          None),
        DumpEntry(
          GlobalDatabaseID(331500787814368353L),
          ScopeID(331576233007513696L),
          DatabaseID(331500787813318753L),
          None),
        DumpEntry(
          ScopeID(309104418431696993L),
          ScopeID(297327065625002081L),
          DatabaseID(309104418430648417L),
          None),
        DumpEntry(
          GlobalDatabaseID(309104418431696993L),
          ScopeID(297327065625002081L),
          DatabaseID(309104418430648417L),
          None)
      )

      val tree = DatabaseTree.build(
        entries,
        snapshotTS = Timestamp.parse("2022-05-13T22:06:23.669999Z"))

      val node = tree.forScopeID(ScopeID(331576233007513696L))

      node.value shouldBe tree
        .forGlobalID(GlobalDatabaseID(331500773353457761L))
        .value

      node.value.children.size shouldBe 4

      node.value.children should contain(
        DatabaseTree(
          ScopeID(331576233007513696L),
          DatabaseID(331500777953561697L),
          ScopeID(331576233007514720L),
          GlobalDatabaseID(331500777953563745L),
          None))

      node.value.children should contain(
        DatabaseTree(
          ScopeID(331576233007513696L),
          DatabaseID(331500788328169569L),
          ScopeID(331576233007515744L),
          GlobalDatabaseID(331500788328171617L),
          None))

      node.value.children should contain(
        DatabaseTree(
          ScopeID(331576233007513696L),
          DatabaseID(331500778495672417L),
          ScopeID(331576233007516768L),
          GlobalDatabaseID(331500778495674465L),
          None))

      node.value.children should contain(
        DatabaseTree(
          ScopeID(331576233007513696L),
          DatabaseID(331500787813318753L),
          ScopeID(331576233007517792L),
          GlobalDatabaseID(331500787814368353L),
          None))
    }

    "error on multiple roots" in {

      //     1      4
      //    /\     /\
      //   2  3   5  6
      val entries = Seq(
        // node 1
        DumpEntry(ScopeID(1), ScopeID(0), DatabaseID(1), None),
        DumpEntry(GlobalDatabaseID(1), ScopeID(0), DatabaseID(1), None),

        // node 2
        DumpEntry(ScopeID(2), ScopeID(1), DatabaseID(2), None),
        DumpEntry(GlobalDatabaseID(2), ScopeID(1), DatabaseID(2), None),

        // node 3
        DumpEntry(ScopeID(3), ScopeID(1), DatabaseID(3), None),
        DumpEntry(GlobalDatabaseID(3), ScopeID(1), DatabaseID(3), None),

        // node 4
        DumpEntry(ScopeID(4), ScopeID(0), DatabaseID(4), None),
        DumpEntry(GlobalDatabaseID(4), ScopeID(0), DatabaseID(4), None),

        // node 5
        DumpEntry(ScopeID(5), ScopeID(4), DatabaseID(5), None),
        DumpEntry(GlobalDatabaseID(5), ScopeID(4), DatabaseID(5), None),

        // node 6
        DumpEntry(ScopeID(6), ScopeID(4), DatabaseID(6), None),
        DumpEntry(GlobalDatabaseID(6), ScopeID(4), DatabaseID(6), None)
      )

      val ex = the[Exception] thrownBy {
        DatabaseTree.build(entries)
      }

      ex.getMessage shouldBe "Multiple root nodes"
    }
  }
}
