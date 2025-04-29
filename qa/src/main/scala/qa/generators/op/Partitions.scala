package fauna.qa.generators.op

import fauna.qa.operator.Cmd.Network
import fauna.qa.{ CoreNode, OpGenerator, QAConfig }
import fauna.qa.operator.{ Annotate, Operation, Pause, Remote }
import scala.concurrent.duration._

/** A RandomPartition drops all network activity between two roughly-equal subgroups of
  * the cluster.
  */
class RandomPartition(config: QAConfig) extends OpGenerator(config) {

  /* Randomly chooses whether a CoreNode should be part of partition 0 or 1. */
  private def f: CoreNode => Int = { _ =>
    config.rand.nextInt(2)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    Partitions.ByGroup(f, nodes)
  }
}

/**
  * A OneDCPartition drops all network activity between one DC and the rest of the cluster.
  * It waits for 5 minutes before partitioning, then maintains the partition for another
  * 5 minutes, then heals the connectivity.
  */
class OneDCPartition(config: QAConfig) extends OpGenerator(config) {

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {

    def getDuration(key: String) =
      config.opt(s"one-dc-partition.$key", QAConfig.parseDuration(_)) getOrElse 5.minutes

    val pre = getDuration("pre")
    val duration = getDuration("duration")

    val victimDC = nodes.head.dc
    Vector.empty :+
      Pause(pre) :+
      Annotate("Partitioning", Vector("DC" -> victimDC)) :++
      Partitions.OneDC(victimDC, nodes) :+
      Pause(duration) :+
      Annotate("Healing", Vector.empty) :+
      Partitions.Heal(nodes)
  }
}

/** A majority ring is a partitioning where every node sees a majority, but
  * different nodes may see different majorities.
  * TODO: I'm not sure this is quite right - should this be "different nodes MUST see
  * a different majority"?
  *
  */
class MajorityRing(config: QAConfig) extends OpGenerator(config) {

  /* Randomly chooses whether a CoreNode should be part of partition 0, 1, or 2. */
  private def f: CoreNode => Int = { _ =>
    config.rand.nextInt(3)
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    /* Nodes will all be able to see roughly 2/3rds of the network */
    Partitions.WithinGroup(f, nodes)
  }
}

object Partitions {

  def ByDC(nodes: Vector[CoreNode]) =
    ByGroup(_.dc, nodes)

  def One(victim: CoreNode, nodes: Vector[CoreNode]) =
    ByGroup(_ == victim, nodes)

  def OneDC(victim: String, nodes: Vector[CoreNode]) =
    ByGroup(_.dc == victim, nodes)

  /** Partition hosts according to some grouping, such that hosts in one group
    * cannot communicate with hosts in any other group. */
  def ByGroup[K](f: CoreNode => K, nodes: Vector[CoreNode]): Vector[Operation] = {
    val byGroup = nodes.groupBy(f).toVector
    for {
      (k, group) <- byGroup
      ourNode <- group
      (_, otherGroup) <- byGroup.filter(_._1 != k)
      otherNode <- otherGroup
    } yield {
      Remote.Drop(otherNode.host.addr)(ourNode.host)
    }
  }

  /** WithinGroup partitions hosts according to some grouping, such that hosts in
    * a group can't communicate with any other node in that group.
    */
  def WithinGroup[K](
    f: CoreNode => K,
    nodes: Vector[CoreNode]
  ): Vector[Operation] = {
    val byGroup = nodes.groupBy(f).toVector
    for {
      (_, group) <- byGroup
      ourNode <- group
      otherNode <- group if otherNode != ourNode
    } yield {
      Remote.Drop(otherNode.host.addr)(ourNode.host)
    }
  }

  def Heal(nodes: Vector[CoreNode]): Operation =
    Remote(Network.Heal, nodes map { _.host })
}
