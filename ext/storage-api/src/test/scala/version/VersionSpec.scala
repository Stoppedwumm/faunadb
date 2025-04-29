package fauna.storage.api.version.test

import fauna.atoms._
import fauna.codex.cbor.CBOR
import fauna.lang.Timestamp
import fauna.storage.{ Create, Delete, Unresolved }
import fauna.storage.api.test._
import fauna.storage.api.version.StorageVersion
import fauna.storage.doc.Data
import fauna.storage.ir.{ MapV, NullV, StringV }
import io.netty.buffer.Unpooled

class VersionSpec extends Spec("version") {

  test("encodes and decodes StorageVersion") {
    val dataBuf = Unpooled.buffer()
    val data = Data(MapV("hello" -> StringV("world")))
    CBOR.encode(dataBuf, data)

    val diffBuf = Unpooled.buffer()
    val diff = NullV
    CBOR.encode(diffBuf, diff)

    val versionBuf = Unpooled.buffer()
    val neuu = StorageVersion.newByteBuf(
      ScopeID(0),
      DocID(SubID(0), CollectionID(0)),
      Unresolved,
      Create,
      SchemaVersion(Timestamp.Epoch),
      null,
      dataBuf,
      diffBuf
    )
    CBOR.encode(versionBuf, neuu)

    // The same but encoded with the old pre-schema-version-and-ttl encoder.
    val oldBuf = Unpooled.wrappedBuffer(
      Array[Byte](
        -121, 0, 74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -58, -10, -56, -10, -12, 77, -95,
        101, 104, 101, 108, 108, 111, 101, 119, 111, 114, 108, 100, 65, -10
      ))

    val neuuAgain =
      TestVersion
        .fromVersion(CBOR.decode[StorageVersion](versionBuf))
        .asInstanceOf[TestVersion.Live]
    val oldAgain =
      TestVersion
        .fromVersion(CBOR.decode[StorageVersion](oldBuf))
        .asInstanceOf[TestVersion.Live]
    neuuAgain.data shouldEqual oldAgain.data
  }

  test("decodes empty data for deleted versions") {
    // Data is nil for a deleted version.
    val dataBuf = Unpooled.buffer()
    val data = NullV
    CBOR.encode(dataBuf, data)

    // This is unrealistic but it's not part of the test.
    val diffBuf = Unpooled.buffer()
    val diff = NullV
    CBOR.encode(diffBuf, diff)

    val v = StorageVersion.newByteBuf(
      ScopeID(0),
      DocID(SubID(0), CollectionID(0)),
      Unresolved,
      Delete,
      SchemaVersion(Timestamp.Epoch),
      null,
      dataBuf,
      diffBuf
    )

    // The data should decode to empty (not throw).
    v.data shouldBe Data.empty
  }
}
