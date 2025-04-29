package fauna.repo.data

/**
  * Result types returned for [[fauna.repo.cassandra.CassandraService]]'s initialization state.
  */
sealed trait InitializeResult
object InitializeResult {

  /**
    * CassandraService is initialised & started
    */
  case object Initialized extends InitializeResult

  /**
    * Replica name has invalid characters.
    */
  case object InvalidReplicaName extends InitializeResult

  /**
    * Replica name already exists (Replica name is immutable).
    */
  case class ReplicaNameAlreadySet(oldReplicaName: String) extends InitializeResult
}
