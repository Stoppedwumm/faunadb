package fauna.lang

trait Service {
  def isRunning: Boolean

  def start(): Unit

  def stop(graceful: Boolean): Unit

  def stop(): Unit = stop(true)

  def die(): Unit = stop(false)
}

case class ServiceManager(services: Seq[Service]) extends Service {

  def isRunning = services exists { _.isRunning }

  def start(): Unit = services foreach { _.start() }

  def stop(graceful: Boolean): Unit = services foreach { _.stop(graceful) }
}
