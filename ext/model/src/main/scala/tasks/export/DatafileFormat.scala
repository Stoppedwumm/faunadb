package fauna.model.tasks.export

import fauna.storage.doc.FieldType
import fauna.storage.ir.StringV

sealed trait DatafileFormat {
  def fileExt(compressed: Boolean) =
    if (compressed) s"$toString.gz" else toString
}

object DatafileFormat {
  final object JSON extends DatafileFormat { override val toString = "json" }
  final object JSONL extends DatafileFormat { override val toString = "jsonl" }

  implicit val fieldType = FieldType[DatafileFormat]("DatafileFormat") { f =>
    StringV(f.toString)
  } {
    case StringV(JSON.toString)  => JSON
    case StringV(JSONL.toString) => JSONL
  }
}
