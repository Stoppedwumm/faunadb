package fauna.auth

import fauna.ast._
import fauna.atoms._
import fauna.lang._
import fauna.repo.doc.Version
import fauna.repo.values.Value
import fauna.storage.doc._
import fauna.storage.index._
import fauna.storage.ir._
import fauna.storage.DocAction

object ActionNames {
  val Read = "read"
  val Call = "call"
  val UnrestrictedRead = "unrestricted_read"
  val Write = "write"
  val Create = "create"
  val CreateWithId = "create_with_id"
  val Delete = "delete"
  val HistoryRead = "history_read"
  val HistoryWrite = "history_write"
  val SchemaRead = "schema_read"
  val SchemaWrite = "schema_write"
  val Logging = "logging"
}

sealed abstract class RoleEvalAction(private[auth] val name: String) {
  val scope: ScopeID

  private[auth] def resource: DocID
  private[auth] def lambdaArgs(arity: Int): Either[Literal, IndexedSeq[Value]]
  private[auth] def fqlxLambdaArgs(arity: Int): Either[Literal, IndexedSeq[Value]] =
    lambdaArgs(arity)
  private[auth] def snapshotTime: Option[Timestamp] = None
}

case class CallFunction(
  scope: ScopeID,
  id: UserFunctionID,
  args: Either[Literal, IndexedSeq[Value]])
    extends RoleEvalAction(ActionNames.Call) {

  private[auth] def resource: DocID = id.toDocID
  private[auth] def lambdaArgs(arity: Int) = args
}

sealed abstract class ReadIndexRoleAction(name: String)
    extends RoleEvalAction(name) {

  val scope: ScopeID
  val id: IndexID
  val terms: Vector[IndexTerm]

  private[auth] def resource: DocID =
    id.toDocID

  private[auth] def lambdaArgs(arity: Int) =
    Left(Literal(scope, ArrayV(terms map { _.value })))
}

case class ReadIndex(scope: ScopeID, id: IndexID, terms: Vector[IndexTerm])
    extends ReadIndexRoleAction(ActionNames.Read)

case class UnrestrictedIndexRead(
  scope: ScopeID,
  id: IndexID,
  terms: Vector[IndexTerm])
    extends ReadIndexRoleAction(ActionNames.UnrestrictedRead)

case class UnrestrictedCollectionRead(scope: ScopeID, id: CollectionID)
    extends RoleEvalAction(ActionNames.UnrestrictedRead) {

  private[auth] def resource: DocID = id.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(NullL)
}

case class ReadInstance(
  scope: ScopeID,
  id: DocID,
  override val snapshotTime: Option[Timestamp] = None)
    extends RoleEvalAction(ActionNames.Read) {

  private[auth] def resource: DocID = id.collID.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(RefL(scope, id))
}

case class WriteInstance(
  scope: ScopeID,
  id: DocID,
  prevVersion: Version.Live,
  newVersion: Version.Live)
    extends RoleEvalAction(ActionNames.Write) {

  private[auth] def resource: DocID =
    id.collID.toDocID

  private[auth] def lambdaArgs(arity: Int) = {
    if (arity == 3) {
      Left(
        ArrayL(
          List(
            Literal(scope, prevVersion.data.fields),
            Literal(scope, newVersion.data.fields),
            RefL(scope, id)
          )))
    } else {
      Left(
        ArrayL(
          List(
            Literal(scope, prevVersion.data.fields),
            Literal(scope, newVersion.data.fields)
          )))
    }
  }

  override private[auth] def fqlxLambdaArgs(arity: Int) = {
    Right(
      IndexedSeq(
        Value.Doc(id),
        Value.Doc(id, versionOverride = Some(newVersion))
      ))
  }
}

// In some cases the user may not want to allow setting the ID, hence the separate permission.
case class CreateInstance(scope: ScopeID, newVersion: Version.Live)
    extends RoleEvalAction(ActionNames.Create) {

  private[auth] def resource: DocID = newVersion.collID.toDocID

  private[auth] def lambdaArgs(arity: Int) =
    Left(Literal(scope, newVersion.data.fields))

  override private[auth] def fqlxLambdaArgs(arity: Int) =
    Right(IndexedSeq(Value.Doc(newVersion.id, versionOverride = Some(newVersion))))
}

case class CreateInstanceWithID(scope: ScopeID, newVersion: Version.Live)
    extends RoleEvalAction(ActionNames.CreateWithId) {

  private[auth] def resource: DocID = newVersion.collID.toDocID

  private[auth] def lambdaArgs(arity: Int) =
    if (arity == 1) {
      Left(Literal(scope, newVersion.data.fields))
    } else {
      Left(
        ArrayL(
          List(
            Literal(scope, newVersion.data.fields),
            RefL(scope, newVersion.id)
          )))
    }

  override private[auth] def fqlxLambdaArgs(arity: Int) =
    Right(IndexedSeq(Value.Doc(newVersion.id, versionOverride = Some(newVersion))))
}

case class DeleteInstance(scope: ScopeID, id: DocID)
    extends RoleEvalAction(ActionNames.Delete) {

  private[auth] def resource: DocID = id.collID.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(RefL(scope, id))
}

case class HistoryRead(scope: ScopeID, id: DocID)
    extends RoleEvalAction(ActionNames.HistoryRead) {

  private[auth] def resource: DocID = id.collID.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(RefL(scope, id))
}

case class HistoryWrite(
  scope: ScopeID,
  id: DocID,
  ts: Timestamp,
  action: DocAction,
  data: Diff)
    extends RoleEvalAction(ActionNames.HistoryWrite) {

  private[auth] def resource: DocID =
    id.collID.toDocID

  private[auth] def lambdaArgs(arity: Int) = {
    Left(
      ArrayL(
        List(
          RefL(scope, id),
          TimeL(ts),
          StringL(action.toString),
          Literal(scope, data.fields)
        )))
  }
}

case class ReadSchema(scope: ScopeID)
    extends RoleEvalAction(ActionNames.SchemaRead) {
  private[auth] def resource: DocID = SchemaSourceID.collID.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(NullL)
}

case class WriteSchema(scope: ScopeID)
    extends RoleEvalAction(ActionNames.SchemaWrite) {
  private[auth] def resource: DocID = SchemaSourceID.collID.toDocID
  private[auth] def lambdaArgs(arity: Int) = Left(NullL)
}

case class Logging(scope: ScopeID) extends RoleEvalAction(ActionNames.Logging) {
  private[auth] def resource: DocID = DocID.MinValue

  private[auth] def lambdaArgs(arity: Int) = Left(NullL)
}
