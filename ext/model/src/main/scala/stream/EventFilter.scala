package fauna.model.stream

import fauna.auth._
import fauna.repo.query._
import fauna.repo.IndexConfig
import fauna.storage.index.{ IndexTerm, NativeIndexID }
import fauna.storage.ops.SetWrite

object EventFilter {

  def check(auth: Auth, event: StreamEvent): Query[Boolean] =
    event match {
      case _: ProtocolEvent      => Query.True
      case NewVersionAdded(w)    => auth.checkReadPermission(w.scope, w.id)
      case HistoryRewrite(w)     => auth.checkHistoryReadPermission(w.scope, w.id)
      case VersionRemoved(w)     => auth.checkHistoryReadPermission(w.scope, w.id)
      case DocumentRemoved(w)    => auth.checkHistoryReadPermission(w.scope, w.id)
      case SetAdded(w, isPart)   => checkPermissionsForSetEvent(auth, w, isPart)
      case SetRemoved(w, isPart) => checkPermissionsForSetEvent(auth, w, isPart)
    }

  private def checkPermissionsForSetEvent(
    auth: Auth,
    write: SetWrite,
    isPartitioned: Boolean): Query[Boolean] = {

    if (write.index == NativeIndexID.DocumentsByCollection.id) {
      auth.checkUnrestrictedReadPermission(write.scope, write.doc.collID) flatMap {
        case false => auth.checkReadPermission(write.scope, write.doc)
        case true  => Query.True
      }
    } else {
      val scalars =
        if (isPartitioned) {
          IndexConfig.dropPartitionKey(write.terms)
        } else {
          write.terms
        }

      val terms = scalars map { IndexTerm(_) }

      auth.checkUnrestrictedReadPermission(write.scope, write.index, terms) flatMap {
        case true => Query.True
        case false =>
          auth.checkReadIndexPermission(write.scope, write.index, terms) flatMap {
            case true  => auth.checkReadPermission(write.scope, write.doc)
            case false => Query.False
          }
      }
    }

  }
}
