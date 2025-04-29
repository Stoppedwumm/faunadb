package fauna.repair.test

import fauna.atoms.{ DatabaseID, GlobalDatabaseID, SchemaVersion, ScopeID }
import fauna.lang.ConsoleControl
import fauna.prop.Prop
import fauna.repair.{ LookupData, LookupsResolver }
import fauna.repo.query.Query
import fauna.repo.test.{ CassandraHelper, PropSpec }
import fauna.repo.Store
import fauna.storage.{ DocAction, Resolved, SetAction }
import fauna.storage.doc.{ Data, Diff }
import fauna.storage.lookup.LiveLookup
import fauna.storage.ops.{ LookupAdd, VersionAdd }
import java.io.PrintWriter
import org.scalatest.BeforeAndAfter
import scala.concurrent.duration._
import scala.concurrent.Await

class LookupResolverSpec extends PropSpec with BeforeAndAfter {
  implicit val ctrl = new ConsoleControl
  val ctx = CassandraHelper.context("repo")
  var lookupsResolver: LookupsResolver = LookupsResolver(
    ctx,
    ctx.service.storageService,
    ctx.service.partitioner.partitioner)

  def newScope = Prop.const(ScopeID(ctx.nextID()))

  before {
    val engine = ctx.service.storage
    val keyspace = engine.keyspace

    /** This makes it such that tests subsequent tests aren't affected by data
      * written by prior ones.
      */
    keyspace.getColumnFamilyStores forEach {
      _.truncateBlocking()
    }
  }

  once("global id lookup with correct version generates no action") {
    for {
      parentScope <- newScope
      scope       <- newScope
      ts          <- Prop.timestamp()
    } {
      val dbID = DatabaseID(2).toDocID
      val globalID = GlobalDatabaseID(1)
      val lookup = LookupAdd(
        globalID = globalID,
        scope = parentScope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = parentScope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID,
            LookupsResolver.ScopeField -> scope))
      )

      Seq(lookup, dbDoc) foreach { write => ctx ! Query.write(write) }

      val origLookup = (ctx ! Store.getLatestLookup(globalID)).get
      origLookup shouldEqual LiveLookup(
        globalID = globalID,
        scope = parentScope,
        id = dbDoc.id,
        ts = Resolved(ts),
        action = SetAction.Add
      )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = parentScope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(fixLookupF, 10.seconds)
      val postLookup = (ctx ! Store.getLatestLookup(globalID)).get
      postLookup shouldEqual origLookup
    }
  }

  once("scope lookup with correct version generates no action") {
    for {
      parentScope <- newScope
      scope       <- newScope
      ts          <- Prop.timestamp()
    } {
      val dbID = DatabaseID(2).toDocID
      val globalID = GlobalDatabaseID(1)
      val lookup = LookupAdd(
        globalID = scope,
        scope = parentScope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = parentScope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID,
            LookupsResolver.ScopeField -> scope))
      )

      Seq(lookup, dbDoc) foreach { write => ctx ! Query.write(write) }
      val origLookup = (ctx ! Store.getLatestLookup(scope)).get
      origLookup shouldEqual LiveLookup(
        globalID = scope,
        scope = parentScope,
        id = dbDoc.id,
        ts = Resolved(ts),
        action = SetAction.Add
      )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = scope,
              scope = parentScope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(fixLookupF, 10.seconds)
      val postLookup = (ctx ! Store.getLatestLookup(scope)).get
      postLookup shouldEqual origLookup
    }
  }

  /** Because database versions are only ever soft deleted, we don't expect this scenario to happen.
    * If it does we leave it as is so we can further investigate.
    */
  once(
    "no decision is made for a lookup entry pointing to a doc that has no versions") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val badGlobalID = GlobalDatabaseID(1)
      val dbID = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = badGlobalID,
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )

      ctx ! Query.write(incorrectLookup)
      val badLookup = (ctx ! Store.getLatestLookup(badGlobalID)).get
      badLookup shouldEqual
        LiveLookup(
          globalID = badGlobalID,
          scope = scope,
          id = dbID,
          ts = Resolved(ts),
          action = SetAction.Add
        )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = badGlobalID,
              scope = scope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(fixLookupF, 10.seconds)

      val postResolvedLookup = (ctx ! Store.getLatestLookup(badGlobalID)).get

      /** since we don't expect this scenario, we leave the data as is.
        */
      postResolvedLookup shouldEqual badLookup
    }
  }

  once(
    "no action is taken when a Remove LookupEntry correctly points to a deleted version") {
    for {
      scope    <- newScope
      ts       <- Prop.timestamp()
      deleteTS <- Prop.timestampAfter(ts)
    } {
      val globalID = GlobalDatabaseID(1)
      val dbID = DatabaseID(2).toDocID
      val removeLookup = LookupAdd(
        globalID = globalID,
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Remove
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID,
            LookupsResolver.ScopeField -> scope))
      )
      val versionDeleted = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(deleteTS),
        action = DocAction.Delete,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = None
      )

      Seq(removeLookup, dbDoc, versionDeleted) foreach { write =>
        ctx ! Query.write(write)
      }

      val preLookup = (ctx ! Store.getLatestLookup(globalID)).get
      preLookup shouldEqual
        LiveLookup(
          globalID = globalID,
          scope = scope,
          id = dbID,
          ts = Resolved(ts),
          action = SetAction.Remove
        )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )

      Await.result(fixLookupF, 10.seconds)
      val postLookup = (ctx ! Store.getLatestLookup(globalID)).get
      postLookup shouldEqual preLookup
    }
  }

  once("lookups entry for deleted versions should be removed") {
    for {
      scope    <- newScope
      ts       <- Prop.timestamp()
      deleteTS <- Prop.timestampAfter(ts)
    } {
      val globalID = GlobalDatabaseID(1)
      val globalID2 = GlobalDatabaseID(2)
      val dbID = DatabaseID(2).toDocID
      val dbID2 = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = globalID,
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val incorrectLookup2 = LookupAdd(
        globalID = globalID2,
        scope = scope,
        id = dbID2,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID,
            LookupsResolver.ScopeField -> scope))
      )
      val versionDeleted = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(deleteTS),
        action = DocAction.Delete,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = None
      )
      val dbDoc2 = VersionAdd(
        scope = scope,
        id = dbID2,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID2,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID2,
            LookupsResolver.ScopeField -> scope))
      )
      val versionDeleted2 = VersionAdd(
        scope = scope,
        id = dbID2,
        writeTS = Resolved(deleteTS),
        action = DocAction.Delete,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID2,
          LookupsResolver.ScopeField -> scope),
        diff = None
      )

      Seq(
        incorrectLookup,
        incorrectLookup2,
        dbDoc,
        versionDeleted,
        dbDoc2,
        versionDeleted2) foreach { write =>
        ctx ! Query.write(write)
      }

      val badLookups = Seq(
        (ctx ! Store.getLatestLookup(globalID)).get,
        (ctx ! Store.getLatestLookup(globalID2)).get)
      badLookups shouldEqual
        Seq(
          LiveLookup(
            globalID = globalID,
            scope = scope,
            id = dbID,
            ts = Resolved(ts),
            action = SetAction.Add
          ),
          LiveLookup(
            globalID = globalID2,
            scope = scope,
            id = dbID2,
            ts = Resolved(ts),
            action = SetAction.Add
          )
        )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = dbID
            ),
            LookupData(
              globalID = globalID2,
              scope = scope,
              docID = dbID2
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )

      Await.result(fixLookupF, 10.seconds)
      val goodLookups = Seq(
        (ctx ! Store.getLatestLookup(globalID)).get,
        (ctx ! Store.getLatestLookup(globalID2)).get)
      goodLookups shouldEqual
        Seq(
          LiveLookup(
            globalID = globalID,
            scope = scope,
            id = dbID,
            ts = Resolved(deleteTS),
            action = SetAction.Remove
          ),
          LiveLookup(
            globalID = globalID2,
            scope = scope,
            id = dbID2,
            ts = Resolved(deleteTS),
            action = SetAction.Remove
          )
        )
    }
  }

  once("no action is taken when dryRun flag is true") {
    for {
      scope    <- newScope
      ts       <- Prop.timestamp()
      deleteTS <- Prop.timestampAfter(ts)
    } {
      val globalID = GlobalDatabaseID(1)
      val dbID = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = globalID,
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> globalID,
            LookupsResolver.ScopeField -> scope))
      )
      val versionDeleted = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(deleteTS),
        action = DocAction.Delete,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> globalID,
          LookupsResolver.ScopeField -> scope),
        diff = None
      )

      Seq(incorrectLookup, dbDoc, versionDeleted) foreach { write =>
        ctx ! Query.write(write)
      }

      val badLookup = (ctx ! Store.getLatestLookup(globalID)).get
      badLookup shouldEqual
        LiveLookup(
          globalID = globalID,
          scope = scope,
          id = dbID,
          ts = Resolved(ts),
          action = SetAction.Add
        )

      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = true
        )

      Await.result(fixLookupF, 10.seconds)

      val postResolvedLookup = (ctx ! Store.getLatestLookup(globalID)).get
      postResolvedLookup shouldEqual badLookup
    }
  }

  /** First want to see if this scenario exists before adding the logic to resolve it.
    * We'd likely look back through the version history to see if the version doc ever
    * had the now incorrect global id, and if so that would give us the desired remove
    * timestamp.
    */
  once(
    "no action is taken for incorrect lookup entry pointing to existing document version") {
    for {
      scope <- newScope
      ts    <- Prop.timestamp()
    } {
      val badGlobalID = GlobalDatabaseID(1)
      val goodGlobalID = GlobalDatabaseID(2)
      val dbID = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = badGlobalID,
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> goodGlobalID,
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> goodGlobalID,
            LookupsResolver.ScopeField -> scope))
      )

      Seq(incorrectLookup, dbDoc) foreach { write => ctx ! Query.write(write) }
      val badLookup = (ctx ! Store.getLatestLookup(badGlobalID)).get
      badLookup shouldEqual
        LiveLookup(
          globalID = badGlobalID,
          scope = scope,
          id = dbID,
          ts = Resolved(ts),
          action = SetAction.Add
        )
      val nextTs = ctx ! Query.snapshotTime
      val resolveLookupsF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = badGlobalID,
              scope = scope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(resolveLookupsF, 10.seconds)

      val postResolvedLookup = (ctx ! Store.getLatestLookup(badGlobalID)).get
      postResolvedLookup shouldEqual badLookup
    }
  }
  // this is because we currently don't take action on adds, typically in the case
  // where it looks like we should make an add it is for a dangling db and we don't
  // want to add a lookup here.  Taking no action here until the tool is equipped
  // to handle this.
  once("no action is taken for incorrect lookup pointing to wrong db id") {
    for {
      scope     <- newScope
      ts        <- Prop.timestamp()
      versionTs <- Prop.timestampAfter(ts)
    } {
      val globalID = GlobalDatabaseID(1)
      val badDbID = DatabaseID(1).toDocID
      val goodDbID = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = GlobalDatabaseID(1),
        scope = scope,
        id = badDbID,
        writeTS = Resolved(ts),
        action = SetAction.Add
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = goodDbID,
        writeTS = Resolved(versionTs),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(
          LookupsResolver.GlobalIDField -> GlobalDatabaseID(1),
          LookupsResolver.ScopeField -> scope),
        diff = Some(
          Diff(
            LookupsResolver.GlobalIDField -> GlobalDatabaseID(1),
            LookupsResolver.ScopeField -> scope))
      )

      Seq(incorrectLookup, dbDoc) foreach { write => ctx ! Query.write(write) }
      val badLookup = ctx ! Store.getLatestLookup(globalID)
      badLookup shouldEqual Some(
        LiveLookup(
          globalID = globalID,
          scope = scope,
          id = badDbID,
          ts = Resolved(ts),
          action = SetAction.Add
        ))
      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = badDbID
            ),
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = goodDbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(fixLookupF, 10.seconds)
      val postFixLookup = ctx ! Store.getLatestLookup(globalID)
      // we don't currently add lookup adds as when this scenario is
      // discovered it is more likely an issue with the database state
      // and we don't want to add a lookup here
      postFixLookup shouldEqual badLookup
    }
  }
  // this is because we currently don't take action on adds, typically in the case
  // where it looks like we should make an add it is for a dangling db and we don't
  // want to add a lookup here.  Taking no action here until the tool is equipped
  // to handle this.
  once("if a lookup remove is present for a live version, no action is taken") {
    for {
      scope     <- newScope
      ts        <- Prop.timestamp()
      versionTs <- Prop.timestampAfter(ts)
    } {
      val globalID = GlobalDatabaseID(1)
      val dbID = DatabaseID(2).toDocID
      val incorrectLookup = LookupAdd(
        globalID = GlobalDatabaseID(1),
        scope = scope,
        id = dbID,
        writeTS = Resolved(ts),
        action = SetAction.Remove
      )
      val dbDoc = VersionAdd(
        scope = scope,
        id = dbID,
        writeTS = Resolved(versionTs),
        action = DocAction.Create,
        schemaVersion = SchemaVersion.Min,
        data = Data(LookupsResolver.GlobalIDField -> GlobalDatabaseID(1)),
        diff = Some(Diff(LookupsResolver.GlobalIDField -> GlobalDatabaseID(1)))
      )

      Seq(incorrectLookup, dbDoc) foreach { write => ctx ! Query.write(write) }
      val badLookup = ctx ! Store.getLatestLookup(globalID)
      badLookup shouldEqual Some(
        LiveLookup(
          globalID = globalID,
          scope = scope,
          id = dbID,
          ts = Resolved(ts),
          action = SetAction.Remove
        ))
      val nextTs = ctx ! Query.snapshotTime
      val fixLookupF =
        lookupsResolver.resolveLookups(
          Seq(
            LookupData(
              globalID = globalID,
              scope = scope,
              docID = dbID
            )
          ),
          nextTs,
          new PrintWriter(System.out),
          dryRun = false
        )
      Await.result(fixLookupF, 10.seconds)
      val postFixLookup = ctx ! Store.getLatestLookup(globalID)
      postFixLookup shouldEqual badLookup
    }
  }
}
