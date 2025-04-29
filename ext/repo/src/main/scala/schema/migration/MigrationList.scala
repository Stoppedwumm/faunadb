package fauna.repo.schema.migration

import fauna.atoms.SchemaVersion
import fauna.storage.doc.Data
import fauna.storage.ir._
import scala.collection.{ SeqView, SortedMap }
import scala.collection.immutable.Queue

object MigrationList {
  def empty = MigrationList(SortedMap.empty[SchemaVersion, List[Migration]])
  def apply(elems: (SchemaVersion, Migration)*): MigrationList = apply(elems.view)
  def apply(elems: SeqView[(SchemaVersion, Migration)]): MigrationList = {
    // This is a rolling list of migrations. We build it up in reverse, and
    // then build the migration map by storing this list while building it.
    // This makes sure the migration map shares the same tail for each element.
    var list: List[Migration] = Nil
    var lastVersion = SchemaVersion.Max
    val result = SortedMap.newBuilder[SchemaVersion, List[Migration]]

    // Reverse the view, so we can build up the latter elements in the map, which are
    // the shorter migration lists, first.
    elems.reverse.foreach { case (version, migration) =>
      // This means Bad Things have happened.
      if (version > lastVersion) {
        throw new IllegalStateException(
          s"found migration out of order (found $version, previous was $lastVersion)")
      }

      if (version < lastVersion) {
        // We want to skip the first element, which would be `SchemaVersion.Max` with
        // an empty list.
        if (lastVersion != SchemaVersion.Max) {
          result += lastVersion -> list
        }
        lastVersion = version
      }

      // Prepend each migration to get the right order out.
      list = migration +: list
    }
    if (lastVersion != SchemaVersion.Max) {
      result += lastVersion -> list
    }

    new MigrationList(result.result())
  }

  // Should only be used for testing.
  def decode(v: IRValue): MigrationList =
    MigrationCodec.migrationListCodec.decode(Some(v), Queue.empty).toOption.get
}

/** This list stores a map of schema versions to complete migration lists.
  *
  * The key is the schema version the first migration is migrating to. So if you
  * have a document on version N, the first migration to apply is the element
  * in the map with the key N + 1 or higher.
  *
  * Storing things in this way makes it simpler to search for migrations, as you
  * can have the same logic if the version N is included in the migration list or
  * not.
  *
  * The value in the map is a linked list which is the complete set of migrations
  * to apply to get to the latest schema version.
  *
  * The constructor is private to repo, as the list of migrations must share tails
  * to encode properly.
  */
final case class MigrationList private[repo] (
  elems: SortedMap[SchemaVersion, List[Migration]])
    extends AnyVal {

  def migrate(data: Data, current: SchemaVersion): Data = if (current.isPending) {
    // A document at the pending schema version is fully migrated.
    data
  } else {
    // `minAfter(N)` will return the key N if it exists. We want the value at N + 1
    // if N exists, so add 1 if that's the case.
    val search =
      if (elems.contains(current)) SchemaVersion(current.toMicros + 1) else current
    elems.minAfter(search) match {
      case Some((_, migrations)) =>
        migrations.foldLeft(data) { case (data, m) => m.migrate(data) }
      case None => data
    }
  }

  private[migration] def encode0(f: (SchemaVersion, Migration) => IRValue) = {
    val result = Vector.newBuilder[IRValue]

    // This iterator always points one element ahead of the `elems.foreach` loop.
    val nextIterator = elems.iterator
    nextIterator.next()

    // Each `migrations` list will contain the complete list from `version` to
    // latest. We want to flatten this into a single list, so loop through
    // `migrations` until the tail equals the next element in `elems`.
    elems.foreach { case (version, migrations) =>
      var list = migrations
      val next = nextIterator.nextOption()
      while (next.forall(_._2 ne list) && list.nonEmpty) {
        result += f(version, list.head)
        list = list.tail
      }
    }

    ArrayV(result.result())
  }

  // Should only be used for testing.
  def encode: IRValue = MigrationCodec.migrationListCodec.encode(this).get

  // This is the schema version used for live writes.
  def latestVersion: SchemaVersion =
    elems.lastOption.map(_._1).getOrElse(SchemaVersion.Min)
}
