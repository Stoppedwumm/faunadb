package fauna.model.runtime.fql2

import fauna.repo.query.ReadCache
import fql.ast.Span
import fql.error.Hint
import fql.error.Hint.HintType

object Hints {
  import ReadCache.CachedDoc.SetSrcHint
  import ReadCache.CachedDoc.SrcHints

  val PerformanceHintLinkPrefix = "https://docs.fauna.com/performance_hint/"
  val FullSetReadCode = "full_set_read"
  val NonCoveredDocumentReadCode = "non_covered_document_read"
  val CollectionScanCode = "collection_scan"

  def NonCoveredIndexRead(
    srcHints: SrcHints,
    path: List[String],
    span: Span): Hint = {
    val indexNames = srcHints.indexSrcHints.map { ih =>
      s"${ih.collectionName}.${ih.indexName}"
    }
    val message = if (indexNames.size == 1) {
      s"""$NonCoveredDocumentReadCode - ${path.mkString(
          ".",
          ".",
          "")} is not covered by the ${indexNames.head} index. See ${performanceHintLink(
          NonCoveredDocumentReadCode)}."""
    } else {
      s"""$NonCoveredDocumentReadCode - `${path.mkString(
          ".",
          ".",
          "")}` is not covered by the following indexes: [${indexNames
          .mkString(",")}]. See ${performanceHintLink(
          NonCoveredDocumentReadCode)}."""
    }
    Hint(
      message = message,
      span = span,
      hintType = HintType.Performance
    )
  }

  private def performanceHintLink(code: String) =
    s"${PerformanceHintLinkPrefix}${code}"

  def FullSetRead(methodName: String, span: Span): Hint = {
    val message =
      s"$FullSetReadCode - Using $methodName() causes the full set to be read. See ${performanceHintLink(FullSetReadCode)}."
    Hint(message, span, hintType = HintType.Performance)
  }

  def IndexSetMaterializedDoc(srcHint: SetSrcHint): Hint = {
    Hint(
      s"$NonCoveredDocumentReadCode - Full documents returned from ${srcHint.collectionName}.${srcHint.indexName}. See ${performanceHintLink(NonCoveredDocumentReadCode)}.",
      srcHint.span,
      hintType = HintType.Performance
    )
  }

  def CollectionScan(collectionName: String, methodName: String, span: Span): Hint =
    Hint(
      s"$CollectionScanCode - Using $methodName() on collection $collectionName can cause a read of every document. See ${performanceHintLink(CollectionScanCode)}.",
      span,
      hintType = HintType.Performance
    )
}
