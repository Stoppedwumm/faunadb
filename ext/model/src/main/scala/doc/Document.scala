package fauna.model

import fauna.ast.EvalContext
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.storage.doc._
import fql.ast.SchemaItem

object Document {

  @inline def DefaultRetainDays =
    SchemaItem.Collection.DefaultHistoryDays.configValue

  val DataValidator: Validator[Query] = DataMapValidator[Query]("data")
  val ConstraintsValidator: Validator[Query] = DataMapValidator("constraints")

  val VersionValidator: Validator[Query] =
    DataValidator +
      ConstraintsValidator +
      Version.TTLField.validator[Query]

  @annotation.nowarn("cat=unused")
  def LiveValidator(ec: EvalContext): Validator[Query] =
    VersionValidator
}

trait Document {
  def version: Version
  def parentScopeID = version.parentScopeID
  def docID = version.docID
  def versionID = version.versionID
}
