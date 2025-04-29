package fauna.storage.test

import fauna.atoms._
import fauna.lang.Timing
import fauna.storage.HashTree
import fauna.codex.cbor._
import java.security.MessageDigest
import java.util.Arrays
import org.scalatest._
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class HashTreeSpec extends AnyFreeSpec
    with LoneElement
    with Matchers {

  "HashTree" - {
    "can build a tree from full ring" in {
      val b = HashTree.newBuilder(Segment.All, 3)
      val sha = Array[Byte](0xCC.toByte, 0xD2.toByte, 0x4C, 0x2E, 0xDB.toByte, 0x3E, 0xF2.toByte, 0xCD.toByte, 0x99.toByte, 0x4B, 0x10, 0x0A, 0x93.toByte, 0xB5.toByte, 0x11, 0xA5.toByte, 0x30, 0x70, 0x42, 0x1B, 0x11, 0x12, 0x1A, 0x7E, 0x5C, 0xA0.toByte, 0x99.toByte, 0x40, 0x92.toByte, 0xBB.toByte, 0x72, 0xC2.toByte)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        b.insert(Location(loc), digest.digest)
      }

      b.result.hash should equal (sha)
    }

    "can build a tree from right half-ring" in {
      val b = HashTree.newBuilder(Segment(Location(0), Location.MinValue), 3)
      val sha = Array[Byte](0xCC.toByte, 0xD2.toByte, 0x4C, 0x2E, 0xDB.toByte, 0x3E, 0xF2.toByte, 0xCD.toByte, 0x99.toByte, 0x4B, 0x10, 0x0A, 0x93.toByte, 0xB5.toByte, 0x11, 0xA5.toByte, 0x30, 0x70, 0x42, 0x1B, 0x11, 0x12, 0x1A, 0x7E, 0x5C, 0xA0.toByte, 0x99.toByte, 0x40, 0x92.toByte, 0xBB.toByte, 0x72, 0xC2.toByte)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        b.insert(Location(loc), digest.digest)
      }

      b.result.hash should equal (sha)
    }

    "can build a tree" in {
      val sha = Array[Byte](0xCC.toByte, 0xD2.toByte, 0x4C, 0x2E, 0xDB.toByte, 0x3E, 0xF2.toByte, 0xCD.toByte, 0x99.toByte, 0x4B, 0x10, 0x0A, 0x93.toByte, 0xB5.toByte, 0x11, 0xA5.toByte, 0x30, 0x70, 0x42, 0x1B, 0x11, 0x12, 0x1A, 0x7E, 0x5C, 0xA0.toByte, 0x99.toByte, 0x40, 0x92.toByte, 0xBB.toByte, 0x72, 0xC2.toByte)
      val seg = Segment(Location(0), Location(10))
      val b = HashTree.newBuilder(seg, 4)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        b.insert(Location(loc), digest.digest)
      }

      b.result.hash should equal (sha)
    }

    "trees with different depths hash the same" in {
      val seg = Segment(Location(0), Location(10))
      val a = HashTree.newBuilder(seg, 3)
      val b = HashTree.newBuilder(seg, 4)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        val hash = digest.digest
        a.insert(Location(loc), hash)
        b.insert(Location(loc), hash)
      }

      a.result.hash should equal (b.result.hash)
    }

    "trees with different data hash differently" in {
      val seg = Segment(Location(0), Location(10))
      val a = HashTree.newBuilder(seg, 4)
      val b = HashTree.newBuilder(seg, 4)

      (0 until 10) foreach { loc =>
        val ad = MessageDigest.getInstance("SHA-256")
        val bd = MessageDigest.getInstance("SHA-256")

        ad.update(Array[Byte](loc.toByte))

        if (loc == 5) {
          bd.update(Array[Byte]((loc + 1).toByte))
        } else {
          bd.update(Array[Byte](loc.toByte))
        }

        a.insert(Location(loc), ad.digest)
        b.insert(Location(loc), bd.digest)
      }

      a.result.hash should not equal (b.result.hash)
    }

    "equal trees do not differ" in {
      val seg = Segment(Location(0), Location(10))
      val a = HashTree.newBuilder(seg, 5)
      val b = HashTree.newBuilder(seg, 5)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        val hash = digest.digest
        a.insert(Location(loc), hash)
        b.insert(Location(loc), hash)
      }

      a.result.diff(b.result).isEmpty should be (true)
      b.result.diff(a.result).isEmpty should be (true)
    }

    "an empty tree differs from a tree with data" in {
      val seg = Segment(Location(0), Location(1))
      val a = HashTree.newBuilder(seg, 1)
      val b = HashTree.newBuilder(seg, 1)

      a.insert(Location(0), Array[Byte](42.toByte)) // chosen by random dice roll

      val diffA = a.result diff b.result
      diffA.loneElement should equal (Segment(Location(0), Location(1)))

      val diffB = b.result diff a.result
      diffB.loneElement should equal (Segment(Location(0), Location(1)))
    }

    "trees with different data differ" in {
      val seg = Segment(Location(0), Location(1))
      val a = HashTree.newBuilder(seg, 1)
      val b = HashTree.newBuilder(seg, 1)

      a.insert(Location(0), Array[Byte](1.toByte))
      b.insert(Location(0), Array[Byte](2.toByte))

      val diffA = a.result diff b.result
      diffA.loneElement should equal (Segment(Location(0), Location(1)))

      val diffB = b.result diff a.result
      diffB.loneElement should equal (Segment(Location(0), Location(1)))
    }

    "trees with similar data differ at the leaves" in {
      val seg = Segment(Location(0), Location(10))
      val a = HashTree.newBuilder(seg, 5)
      val b = HashTree.newBuilder(seg, 5)

      a.insert(Location(0), Array[Byte](1.toByte))
      b.insert(Location(0), Array[Byte](1.toByte))
      a.insert(Location(9), Array[Byte](2.toByte))
      b.insert(Location(9), Array[Byte](3.toByte))

      val diffA = a.result diff b.result
      diffA.loneElement should equal (Segment(Location(8), Location(10)))

      val diffB = b.result diff a.result
      diffB.loneElement should equal (Segment(Location(8), Location(10)))
    }

    "trees with contiguous differences yield fewer segments" in {
      val seg = Segment(Location(0), Location(10))
      val a = HashTree.newBuilder(seg, 5)
      val b = HashTree.newBuilder(seg, 5)

      a.insert(Location(0), Array[Byte](1.toByte))
      b.insert(Location(0), Array[Byte](1.toByte))
      a.insert(Location(5), Array[Byte](2.toByte))
      b.insert(Location(6), Array[Byte](3.toByte))
      a.insert(Location(9), Array[Byte](4.toByte))
      b.insert(Location(9), Array[Byte](4.toByte))

      val diffA = a.result diff b.result
      diffA.loneElement should equal (Segment(Location(5), Location(7)))

      val diffB = b.result diff a.result
      diffB.loneElement should equal (Segment(Location(5), Location(7)))
    }

    "encodes and decodes" in {
      val seg = Segment(Location(0), Location(10))
      val b = HashTree.newBuilder(seg, 4)

      (0 until 10) foreach { loc =>
        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(Array[Byte](loc.toByte))
        b.insert(Location(loc), digest.digest)
      }

      val tree = b.result

      CBOR.decode[HashTree](CBOR.encode(tree)).segment should equal (tree.segment)
      CBOR.decode[HashTree](CBOR.encode(tree)).hash should equal (tree.hash)
      CBOR.decode[HashTree](CBOR.encode(tree)).diff(tree).isEmpty should be (true)
    }

    "stores many hashes" in {
      val depth = 20
      val entries = 1_000_0000
      val arr = new Array[Byte](1024)
      val digest = MessageDigest.getInstance("SHA-256")
      val locs = (0 until entries) map { _ => Location.random() } sorted

      val b = HashTree.newBuilder(Segment.All, 5)
      val t = Timing.start

      locs foreach { loc =>
        // this isn't an array-filling test, but the fill appears to
        // have a minor effect on the total throughput; thanks JVM!
        Arrays.fill(arr, loc.token.toByte)
        digest.update(arr)
        b.insert(loc, digest.digest)
      }

      val elapsed = t.elapsedMillis.toFloat / 1000
      val tput = entries / elapsed
      info(s"$entries entries (x ${arr.size}B) @ depth $depth: $elapsed secs $tput entries/sec")
    }

  }
}
