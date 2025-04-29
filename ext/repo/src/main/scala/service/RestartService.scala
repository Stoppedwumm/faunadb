package fauna.repo.service

import fauna.atoms.HostID

trait RestartService {
  final class Handle

  /** Registers a function which will be called when a peer
    * (re)starts.
    *
    * The handle returned may be used to unsubscribe.
    */
  def subscribeStartsAndRestarts(f: HostID => Unit): Handle

  /** Deregisters a function registered with the provided handle. */
  def unsubscribeStartsAndRestarts(handle: Handle): Unit
}
