package fauna.storage.api.test

import fauna.lang._
import fauna.lang.syntax._
import fauna.prop.Prop
import fauna.storage.api.RowMerger
import fauna.storage.Cell
import scala.concurrent.Future

class RowMergerSpec extends Spec("rowmerger") {

  prop("count bytes read when reducing streams") {
    for {
      nCells <- Prop.int(4 to 64)
      names  <- Prop.string() * nCells
      values <- Prop.string() * nCells
    } yield {
      val cells =
        names.zip(values) map { case (name, value) =>
          Cell(name.toUTF8Buf, value.toUTF8Buf, Timestamp.Epoch)
        }

      val merger =
        RowMerger(
          columnFamily = "FakeCF",
          diskCells = Future.successful(Page(cells)),
          writeIntents = Seq.empty
        )

      val reduced =
        merger.reduceStream(Option.empty[RowMerger.Entry[Cell]]) {
          case (None, None)       => (None, None)
          case (Some(a), None)    => (None, Some(a))
          case (Some(a), Some(b)) => (Some(b), Some(a))
          case (None, Some(b))    => (Some(b), None)
        }

      val result = await(reduced.flatten())
      val totalBytes = cells map { _.byteSize } sum

      result.bytesEmitted shouldBe totalBytes // don't miss any bytes
    }
  }
}
