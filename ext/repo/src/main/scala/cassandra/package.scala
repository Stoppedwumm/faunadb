package fauna.repo

import fauna.storage.ScanBounds
import fauna.storage.cassandra.comparators.{
  AbstractCBORType,
  CBORType => RepoCBORType
}
import io.netty.buffer.ByteBuf
import org.apache.cassandra.db.marshal.AbstractType

package cassandra {

  package comparators {

    /**
      * Note: this type can't be removed, even if it is duplicated as fauna.storage.cassandra.comparators.CBORType.
      * This is because cassandra persists the name of this type and tries to find it using reflection, thus removing
      * it would break existing installs.
      * */
    object CBORType extends AbstractCBORType {
      override def isCompatibleWith(other: AbstractType[_ <: Object]): Boolean =
        this.equals(other) || other.equals(RepoCBORType)
    }
  }
}

package object cassandra {

  type RowSliceReadCursor = (ByteBuf, ByteBuf, Boolean)
  type RangeReadCursor = (ScanBounds, ByteBuf, Boolean) // (bounds, cell, excludeStart)
}
