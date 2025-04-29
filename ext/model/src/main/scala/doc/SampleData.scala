package fauna.model

import fauna.atoms.SampleDataID
import fauna.lang.syntax._
import fauna.lang.Timestamp
import fauna.model.schema.InternalCollection
import fauna.repo.doc._
import fauna.storage.doc._

case class SampleData(version: Version) extends Document {
  val id = docID.as[SampleDataID]
}

object SampleData {

  private val SampleDataColl =
    InternalCollection.SampleData(Database.RootScopeID)

  /** These ids were generated so that regardless of the partition boundaries, there is a high likelihood that
    * a set of documents with these IDs will span all 3 nodes in a 3 node replica even after repartitioning.
    * The likelihood that this set will span the entire replica drops as the replica size increases.
    *
    * This set is primarily used by `ping` to check the health of the read pipeline.
    */
  val pseudorandomIDs = Seq(
    8092012945730612511L, 3039003014734614082L, 5709680239063633193L,
    6962659816442215250L, 8495487159308778140L, 2294138817672856424L,
    5822139588624865104L, 3097776429123688382L, 3434035391588538060L,
    5666018734065034034L, 6423323294720019363L, 6378147735429740145L,
    1287473393463081882L, 2285835617059634234L, 3192016342982935302L,
    3695584223375198355L
  ) map { SampleDataID(_) }

  def check(snapshotTS: Timestamp) =
    (pseudorandomIDs map { get(_, snapshotTS) }).sequence map {
      _ forall { _.isDefined }
    }

  private[this] def get(id: SampleDataID, snapshotTS: Timestamp) =
    SampleDataColl.get(id, snapshotTS) mapT { apply(_) }

  private[this] def update(id: SampleDataID) =
    SampleDataColl.insert(id, Data.empty, true)

  def init() = pseudorandomIDs map { update(_) } join
}
