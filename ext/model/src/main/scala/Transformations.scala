package fauna.model

import fauna.atoms.{ APIVersion, DocID, IndexID }
import fauna.repo.doc.Version
import fauna.storage.ir.{ ArrayV, MapV }

object Transformations {

  // may need different types of transformations in the future
  trait APITrans {
    def versions(vers: APIVersion): Boolean
    def typeCheck(typ: DocID): Boolean
    def fn(m0: MapV): MapV
    def matches(vers: APIVersion, typ: DocID): Boolean =
      versions(vers) && typeCheck(typ)
  }

  object ClassColIdxSrcTrans extends APITrans {
    def versions(vers: APIVersion) = vers >= APIVersion.V27
    def typeCheck(typ: DocID) = typ.is[IndexID]
    def fn(m0: MapV) = {
      m0.get(Index.SourceField.path) flatMap {
        case ArrayV(elems) =>
          val newSources = elems map {
            case m: MapV =>
              m.rename(List("class"), "collection")

            case v =>
              v
          }
          Option(m0.update(Index.SourceField.path, ArrayV(newSources)))

        case _: MapV =>
          Some(m0.rename(Index.SourceField.path ::: List("class"), "collection"))

        case _ =>
          None
      } getOrElse m0
    }
  }

  object ColClassIdxSrcTrans extends APITrans {
    def versions(vers: APIVersion) = vers < APIVersion.V27
    def typeCheck(typ: DocID) = typ.is[IndexID]
    def fn(m0: MapV) = {
      m0.get(Index.SourceField.path) flatMap {
        case ArrayV(elems) => {
          val newSources = elems map {
            case m: MapV =>
              m.rename(List("collection"), "class")

            case v =>
              v
          }

          Option(m0.update(Index.SourceField.path, ArrayV(newSources)))
        }

        case _: MapV =>
          Some(m0.rename(Index.SourceField.path ::: List("collection"), "class"))

        case _ =>
          None
      } getOrElse m0
    }
  }

  // add all newly defined transformations here. They are applied first -> last
  val AllAPITransformations = List(ClassColIdxSrcTrans, ColClassIdxSrcTrans)

  def transformToVersion(vers: APIVersion, all: List[APITrans], v: Version): MapV =
    all
      .filter { _.matches(vers, v.docID) }
      .foldLeft[MapV => MapV](identity) { (b, a) => b compose a.fn }(v.data.fields)

}
