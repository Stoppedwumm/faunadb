package fauna.qa.generators.test.correctness

import fauna.qa.QAConfig

class TestConfig(config: QAConfig) {
  val StreamCount = config.getInt("correctness.stream-count")

  val CreateObjectsRatio = config.getInt("correctness.query-ratios.create-objects")
  val UpdateObjectsRatio = config.getInt("correctness.query-ratios.update-objects")
  val DeleteObjectsRatio = config.getInt("correctness.query-ratios.delete-objects")
  val ReadObjectsRatio = config.getInt("correctness.query-ratios.read-objects")

  val MaxInstancePadding = config.getInt("correctness.max-instance-padding")

  val IndexAndVerify = config.getBoolean("correctness.index-and-verify")

  val VerifyIndexesRatio =
    if (IndexAndVerify) config.getInt("correctness.query-ratios.verify-indexes")
    else 0

  val CreateWithConstraintViolationRatio =
    if (IndexAndVerify)
      config.getInt("correctness.query-ratios.create-with-constraint-violation")
    else 0

  val DeleteWithConstraintViolationRatio =
    if (IndexAndVerify)
      config.getInt("correctness.query-ratios.delete-with-constraint-violation")
    else 0

  val MinRememberedObjects = config.getInt("correctness.min-remembered-objects")
  val MaxRememberedObjects = config.getInt("correctness.max-remembered-objects")
  val MaxCreatedObjects = config.getInt("correctness.max-created-objects")
  val MaxDeletedObjects = config.getInt("correctness.max-deleted-objects")

  val CreateObjectsThreshold = CreateObjectsRatio
  val UpdateObjectsThreshold = CreateObjectsThreshold + UpdateObjectsRatio
  val DeleteObjectsThreshold = UpdateObjectsThreshold + DeleteObjectsRatio
  val ReadObjectsThreshold = DeleteObjectsThreshold + ReadObjectsRatio
  val VerifyIndexesThreshold = ReadObjectsThreshold + VerifyIndexesRatio

  val CreateWithConstraintViolationThreshold =
    VerifyIndexesThreshold + CreateWithConstraintViolationRatio

  val DeleteWithConstraintViolationThreshold =
    CreateWithConstraintViolationThreshold + DeleteWithConstraintViolationRatio

  val TotalRatios = DeleteWithConstraintViolationThreshold
}
