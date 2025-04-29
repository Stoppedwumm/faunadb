package fauna.net.bus

class UnboundBusServiceException extends Exception(
  "BusService unbound. Call `bind(bus: MessageBus)` before use.")

trait BusService {
  def bind(bus: MessageBus): Unit
  def unbind(): Option[MessageBus]
}

trait AggregateBusService {
  protected def busServices: Seq[BusService]

  def bind(bus: MessageBus): Unit =
    busServices foreach { _.bind(bus) }

  def unbind() =
    (busServices foldLeft Option.empty[MessageBus]) { (_, d) => d.unbind() }
}

trait BusConsumer extends BusService {
  private[this] var _bus: MessageBus = null

  protected def bus: MessageBus = {
    if (_bus eq null) throw new UnboundBusServiceException
    _bus
  }

  def bind(bus: MessageBus): Unit = _bus = bus

  def unbind(): Option[MessageBus] = {
    val rv = Option(_bus)
    _bus = null
    rv
  }
}
