package fauna.repo.service.test

import fauna.atoms._
import fauna.flags._
import fauna.limits._
import fauna.repo.test._
import fauna.stats.QueryMetrics

class AccountLimitsSpec extends Spec {
  val Account = AccountID(42)
  val DefaultReads = new RateLimitsReads(100)
  val DefaultWrites = new RateLimitsWrites(100)
  val DefaultCompute = new RateLimitsCompute(100)

  "AccountLimits" - {
    "hard limits" - {
      "defaults" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = None,
          hardWrite = None,
          hardCompute = None,
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(DefaultReads.default.value)
        hard.read.burstSeconds should equal(10.0)

        hard.write.permitsPerSecond should equal(DefaultWrites.default.value)
        hard.write.burstSeconds should equal(10.0)

        hard.compute.permitsPerSecond should equal(
          DefaultCompute.default.value * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(10.0)
      }

      "prefer flags to settings" in {
        val flags = AccountFlags(
          Account,
          Map(
            DefaultReads.key -> 1.0,
            DefaultWrites.key -> 2.0,
            DefaultCompute.key -> 3.0))

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(1.0)
        hard.read.burstSeconds should equal(10.0)

        hard.write.permitsPerSecond should equal(2.0)
        hard.write.burstSeconds should equal(10.0)

        hard.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(10.0)
      }

      "prefer settings to defaults" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(10.0)
        hard.write.burstSeconds should equal(10.0)

        hard.write.permitsPerSecond should equal(10.0)
        hard.write.burstSeconds should equal(10.0)

        hard.compute.permitsPerSecond should equal(
          10.0 * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(10.0)
      }

      "limit defaults to system limits" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = None,
          hardWrite = None,
          hardCompute = None,
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 1.0,
          systemWriteLimit = 2.0,
          systemComputeLimit = 3.0,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(1.0)
        hard.read.burstSeconds should equal(0.0)

        hard.write.permitsPerSecond should equal(2.0)
        hard.write.burstSeconds should equal(0.0)

        hard.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(0.0)
      }

      "limit flags to system limits" in {
        val flags = AccountFlags(
          Account,
          Map(
            DefaultReads.key -> 100_000.0,
            DefaultWrites.key -> 200_000.0,
            DefaultCompute.key -> 300_000.0))

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 1.0,
          systemWriteLimit = 2.0,
          systemComputeLimit = 3.0,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(1.0)
        hard.read.burstSeconds should equal(0.0)

        hard.write.permitsPerSecond should equal(2.0)
        hard.write.burstSeconds should equal(0.0)

        hard.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(0.0)
      }

      "limit settings to system limits" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 1.0,
          systemWriteLimit = 2.0,
          systemComputeLimit = 3.0,
          burstSeconds = 10.0)

        hard.read.permitsPerSecond should equal(1.0)
        hard.read.burstSeconds should equal(0.0)

        hard.write.permitsPerSecond should equal(2.0)
        hard.write.burstSeconds should equal(0.0)

        hard.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        hard.compute.burstSeconds should equal(0.0)
      }
    }

    "soft limits" - {
      "defaults" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = None,
          hardWrite = None,
          hardCompute = None,
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(hard.read.permitsPerSecond)
        soft.read.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(hard.write.permitsPerSecond)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(hard.compute.permitsPerSecond)
        soft.compute.burstSeconds should equal(0.0)
      }

      "prefer flags to settings" in {
        val flags = AccountFlags(
          Account,
          Map(
            DefaultReads.key -> 1.0,
            DefaultWrites.key -> 2.0,
            DefaultCompute.key -> 3.0))

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = Some(5.0),
          softWrite = Some(5.0),
          softCompute = Some(5.0))

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(1.0)
        soft.read.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(2.0)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        soft.compute.burstSeconds should equal(0.0)
      }

      "prefer settings to defaults" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = Some(5.0),
          softWrite = Some(5.0),
          softCompute = Some(5.0))

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(5.0)
        soft.write.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(5.0)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(
          5.0 * QueryMetrics.BaselineCompute)
        soft.compute.burstSeconds should equal(0.0)
      }

      "limit defaults to hard limits" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = Some(1.0),
          hardWrite = Some(2.0),
          hardCompute = Some(3.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(1.0)
        soft.read.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(2.0)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        soft.compute.burstSeconds should equal(0.0)
      }

      // This is somewhat contrived: both soft and hard should observe
      // the same flags, but if for some reason they deviate...
      "limit flags to hard limits" in {
        val flags = AccountFlags(
          Account,
          Map(
            DefaultReads.key -> 100_000.0,
            DefaultWrites.key -> 200_000.0,
            DefaultCompute.key -> 300_000.0))

        val limits = AccountLimits(
          hardRead = Some(1.0),
          hardWrite = Some(2.0),
          hardCompute = Some(3.0),
          softRead = None,
          softWrite = None,
          softCompute = None)

        val hard = limits.getHardLimits(
          AccountFlags(Account, Map.empty), // NB. using settings.
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0
        )

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(1.0)
        soft.read.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(2.0)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(
          3.0 * QueryMetrics.BaselineCompute)
        soft.compute.burstSeconds should equal(0.0)
      }

      "limit settings to hard limits" in {
        val flags = AccountFlags(Account, Map.empty)

        val limits = AccountLimits(
          hardRead = Some(10.0),
          hardWrite = Some(10.0),
          hardCompute = Some(10.0),
          softRead = Some(100.0),
          softWrite = Some(100.0),
          softCompute = Some(100.0))

        val hard = limits.getHardLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          systemReadLimit = 100_000,
          systemWriteLimit = 100_000,
          systemComputeLimit = 100_000,
          burstSeconds = 10.0)

        val soft = limits.getSoftLimits(
          flags,
          DefaultReads,
          DefaultWrites,
          DefaultCompute,
          hard)

        soft.read.permitsPerSecond should equal(10.0)
        soft.read.burstSeconds should equal(0.0)

        soft.write.permitsPerSecond should equal(10.0)
        soft.write.burstSeconds should equal(0.0)

        soft.compute.permitsPerSecond should equal(
          10.0 * QueryMetrics.BaselineCompute)
        soft.compute.burstSeconds should equal(0.0)
      }
    }
  }
}
