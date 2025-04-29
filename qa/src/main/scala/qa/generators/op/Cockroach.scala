package fauna.qa.generators.op

import fauna.qa.operator.{ Operation, Remote, SlackMsg }
import fauna.qa.{ CoreNode, OpGenerator, QAConfig }
import scala.concurrent.duration._

// Obviously this name is stupid
sealed trait OpGeneratorGenerator {
  val name: String
  val init: () => OpGenerator
}

/* A Nemesis is a Cockroach Jepsen op, and our implementation of it.  We subclass
 * skews in order to ensure that we not simultaneously test two clockskews.
 */
private case class Nemesis(name: String, init: () => OpGenerator)
    extends OpGeneratorGenerator

private case class SkewNemesis(name: String, init: () => OpGenerator)
    extends OpGeneratorGenerator

/* CockroachNemeses is a port of the ops that CockroachSchema.DB's Jepsen tests have
 * implemented, as defined here https://github.com/jepsen-io/jepsen/tree/master/cockroachdb.
 *
 * In particular, note that "Jepsen will test every combination of (the nemeses),
 * except where both nemeses would be identical, or both would introduce clock skew.
 *
 * Note that this generator randomly chooses two nemeses instead of just running
 * through each pair in turn.  Making it do the latter requires some changes to
 * the QA system, so instead we expect that multiple independent runs will help
 * explore the solution space.  Since we can't have two skews or two identical
 * nemeses, or that (a,b) = (b,a), we have:
 *  (num of non-skew nemeses) * (num of non-skew nemeses - 1) / 2 +  //0 skews
 *  (num of non-skew nemeses) * (num of skew nemeses)         / 2 +  //1 skew
 *  = 6 * 5 + 6 * 6 / 2 = 33 possible combinations, which is not super large.
 */
class CockroachJepsenOp(config: QAConfig) extends OpGenerator(config) {

  private val nemeses: Vector[OpGeneratorGenerator] = Vector(
    SkewNemesis("none", { () =>
      new Nop(config)
    }),
    SkewNemesis("small-skews", { () =>
      ClockSkew.skew(config, 100 millis)
    }),
    SkewNemesis("subcritical-skews", { () =>
      ClockSkew.skew(config, 200 millis)
    }),
    SkewNemesis("critical-skews", { () =>
      ClockSkew.skew(config, 250 millis)
    }),
    SkewNemesis("big-skews", { () =>
      ClockSkew.skew(config, 500 millis)
    }),
    SkewNemesis("huge-skews", { () =>
      ClockSkew.skew(config, 5 seconds)
    }),
    SkewNemesis("strobe-skews", { () =>
      ClockSkew.strobe(config, 200 millis, 1000)
    }),
    Nemesis("parts", { () =>
      new RandomPartition(config)
    }),
    Nemesis("majority-ring", { () =>
      new MajorityRing(config)
    }),
    Nemesis(
      "start-stop-2", { () =>
        new OpGenerator(config) {
          val hangDuration = 1 minute //TODO: right value?
          override def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
            Remote.HangCores(nRandom(nodes, 2), hangDuration)
          }
        }
      }
    ),
    Nemesis("start-kill-2", { () =>
      new OpGenerator(config) {
        override def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
          Remote.KillCores(nRandom(nodes, 2))
        }
      }
    }),
    Nemesis(
      "slow", { () =>
        new OpGenerator(config) {
          val delay = 100 millis //TODO: right value?
          val jitter = 50 millis //TODO: right value?
          val saga = Remote.Slow(delay, jitter)

          override def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
            nodes map { n =>
              saga(n.host)
            }
          }
        }
      }
    )
  )

  @annotation.tailrec
  private def twoNemeses(): (OpGeneratorGenerator, OpGeneratorGenerator) = {
    (config.rand.shuffle(nemeses).take(2): @unchecked) match {
      case Vector(_: SkewNemesis, _: SkewNemesis) => twoNemeses()
      case Vector(t1, t2) if t1 == t2             => twoNemeses()
      case Vector(t1, t2)                         => (t1, t2)
    }
  }

  def ops(nodes: Vector[CoreNode]): Vector[Operation] = {
    val ops = nodes flatMap { n =>
      val (tg1, tg2) = twoNemeses()
      val msg = SlackMsg(
        "CockroachJepsenTest",
        Vector("Nemesis 1" -> tg1.name, "Nemesis 2" -> tg2.name)
      )

      Vector(msg) ++
        tg1.init().ops(Vector(n)) ++
        tg2.init().ops(Vector(n))
    }
    ops.toVector
  }
}
