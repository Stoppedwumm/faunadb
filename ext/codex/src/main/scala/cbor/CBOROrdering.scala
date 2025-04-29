package fauna.codex.cbor

import com.ibm.icu.impl.coll.CollationData
import com.ibm.icu.text.Collator
import fauna.lang.syntax._
import io.netty.buffer.ByteBuf
import java.lang.{ Double => JDouble, Float => JFloat, Long => JLong }
import java.nio.charset.CharacterCodingException

object CBOROrdering {

  // A Collator for the locale returned by
  // java.util.Locale.getDefault(); frozen for safety.
  private[this] val DefaultCollator = new ThreadLocal[Collator] {
    override protected def initialValue() = Collator.getInstance.cloneAsThawed()
  }

  private[this] val CollatorData = new ThreadLocal[CollationData] {
    override protected def initialValue() = {
      val dataf = DefaultCollator.get.getClass.getDeclaredField("data")
      dataf.setAccessible(true)
      dataf.get(DefaultCollator.get).asInstanceOf[CollationData]
    }
  }

  private[this] val log = getLogger

  @inline private def isUTF8ContChar(byte: Byte) = (byte & 0xC0) == 0x80

  @inline private def isUnsafeBackward(c: Char) = CollatorData.get.isUnsafeBackward(c, false)

  private def readInt(byte: Long, buf: ByteBuf): Long =
    // FIXME It's not really an Int anymore, is it
    if (byte < InitialByte.UInt8) byte
    else if (byte >= InitialByte.NegIntMin) -1 - readInt(byte & CBORParser.InfoMask, buf)
    else
      ((byte & CBORParser.InfoMask): @unchecked) match {
        case InitialByte.UInt8  => buf.readUnsignedByte
        case InitialByte.UInt16 => buf.readUnsignedShort
        case InitialByte.UInt32 => buf.readUnsignedInt
        case InitialByte.UInt64 =>
          val long = buf.readLong
          require(long >= 0)
          long
      }

  private def readStringLen(byte: Long, buf: ByteBuf): Int =
    (byte: @unchecked) match {
      case b if b >= InitialByte.StrMin && b <= InitialByte.StrMax =>
        (b - InitialByte.StrMin).toInt
      case InitialByte.UInt8Str  => buf.readUnsignedByte
      case InitialByte.UInt16Str => buf.readUnsignedShort
      case InitialByte.UInt32Str =>
        val int = buf.readUnsignedInt
        require(int <= Int.MaxValue)
        int.toInt
      case InitialByte.UInt64Str =>
        val long = buf.readLong
        require(long >= 0 && long <= Int.MaxValue)
        long.toInt
    }

  def equals(a: ByteBuf, b: ByteBuf): Boolean = a == b

  def compare(a: ByteBuf, b: ByteBuf): Int =
    if (a == b) {
      0
    } else {
      val buf1 = a.duplicate
      val buf2 = b.duplicate

      try {
        if (log.isDebugEnabled) {
          buf1.markReaderIndex
          buf2.markReaderIndex
        }
        compare0(buf1, buf2)
      } catch {
        case t: Throwable =>
          if (log.isDebugEnabled) {
            buf1.resetReaderIndex
            buf2.resetReaderIndex
            log.debug(s"Exception caught in CBOROrdering while comparing:\n\t${buf1.toHexString}\n\t${buf2.toHexString}")
          }
          throw t
      }
    }

  private def isFloatingPoint(byte: Int) = {
    val info = byte & CBORParser.InfoMask
    info == TypeInfos.FloatInfo || info == TypeInfos.DoubleInfo
  }

  private def compare0(buf1: ByteBuf, buf2: ByteBuf): Int = {
    val ib1 = buf1.readUnsignedByte
    val ib2 = buf2.readUnsignedByte

    val mt1 = ib1 >> CBORParser.MajorTypeBits
    val mt2 = ib2 >> CBORParser.MajorTypeBits

    (mt1: @annotation.switch) match {
      case MajorType.PositiveInt =>
        mt2 match {
          case MajorType.PositiveInt                  => compareInts(ib1, buf1, ib2, buf2)
          case MajorType.NegativeInt                  => 1
          case MajorType.Misc if isFloatingPoint(ib2) => compareIntNumber(ib1, buf1, ib2, buf2)
          case _                                      => compareMajorTypes(mt1, ib1, mt2, ib2)
        }

      case MajorType.NegativeInt =>
        mt2 match {
          case MajorType.NegativeInt                  => compareInts(ib1, buf1, ib2, buf2)
          case MajorType.PositiveInt                  => -1
          case MajorType.Misc if isFloatingPoint(ib2) => compareIntNumber(ib1, buf1, ib2, buf2)
          case _                                      => compareMajorTypes(mt1, ib1, mt2, ib2)
        }

      case MajorType.ByteString =>
        if (mt2 == MajorType.ByteString) compareBytes(ib1, buf1, ib2, buf2) else compareMajorTypes(mt1, ib1, mt2, ib2)

      case MajorType.UTF8String =>
        if (mt2 == MajorType.UTF8String) compareStrings(ib1, buf1, ib2, buf2) else compareMajorTypes(mt1, ib1, mt2, ib2)

      case MajorType.Array =>
        if (mt2 == MajorType.Array) compareArrays(ib1, buf1, ib2, buf2) else compareMajorTypes(mt1, ib1, mt2, ib2)

      case MajorType.Map =>
        if (mt2 == MajorType.Map) compareMaps(ib1, buf1, ib2, buf2) else compareMajorTypes(mt1, ib1, mt2, ib2)

      case MajorType.Tag =>
        if (mt2 == MajorType.Tag) compareTagged(ib1, buf1, ib2, buf2) else compareMajorTypes(mt1, ib1, mt2, ib2)

      case MajorType.Misc =>
        mt2 match {
          case MajorType.PositiveInt if isFloatingPoint(ib1) => -compareIntNumber(ib2, buf2, ib1, buf1)
          case MajorType.NegativeInt if isFloatingPoint(ib1) => -compareIntNumber(ib2, buf2, ib1, buf1)
          case MajorType.Misc                                => compareMisc(ib1, buf1, ib2, buf2)
          case _                                             => compareMajorTypes(mt1, ib1, mt2, ib2)
        }
    }
  }

  //remap float point and put it before strings, array, map, and nulls
  private def compareMajorTypes(mt1: Int, ib1: Int, mt2: Int, ib2: Int): Int = {
    if (mt1 == MajorType.Misc && isFloatingPoint(ib1)) {
      -1
    } else if (mt2 == MajorType.Misc && isFloatingPoint(ib2)) {
      +1
    } else {
      Integer.compare(mt1, mt2)
    }
  }

  private def compareInts(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf): Int = {
    val i1 = readInt(ib1, buf1)
    val i2 = readInt(ib2, buf2)
    JLong.compare(i1, i2)
  }

  private def compareBytes(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf): Int = {
    val vl1 = ib1 & CBORParser.InfoMask
    val vl2 = ib2 & CBORParser.InfoMask
    val len1 = readInt(vl1, buf1).toInt
    val len2 = readInt(vl2, buf2).toInt

    val min = Math.min(len1, len2)
    val i = buf1.mismatchIndex(buf2, min)

    val byteIndex1 = buf1.readerIndex + i
    val byteIndex2 = buf2.readerIndex + i

    // Advance readerIndex to the ends of the objects
    buf1.skipBytes(len1)
    buf2.skipBytes(len2)

    if (i == min) {
      Integer.compare(len1, len2)
    } else {
      Integer.compare(buf1.getUnsignedByte(byteIndex1), buf2.getUnsignedByte(byteIndex2))
    }
  }

  private def compareStrings(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf): Int = {
    val len1 = readStringLen(ib1, buf1)
    val len2 = readStringLen(ib2, buf2)

    val min = Math.min(len1, len2)
    val first = buf1.mismatchIndex(buf2, min)

    val result = if ((first == len1) || (first == len2)) {
      // One string is a prefix of the other; sort by length.
      Integer.compare(len1, len2)
    } else {
      var i = first
      // Backtrack to find the first complete unicode char, in case the char has a
      // partial leading byte match
      while (isUTF8ContChar(buf1.getByte(buf1.readerIndex + i))) i -= 1

      var cs1 = new LazyUTF8Sequence(buf1, i, len1 - i)
      var cs2 = new LazyUTF8Sequence(buf2, i, len2 - i)

      // If either of the first characters is unsafe as a comparison starting point,
      // backtrack to the first safe character.
      while (i > 0 && (isUnsafeBackward(cs1.charAt(0)) || isUnsafeBackward(cs2.charAt(0)))) {
        i -= 1
        while (isUTF8ContChar(buf1.getByte(buf1.readerIndex + i))) i -= 1

        cs1 = new LazyUTF8Sequence(buf1, i, len1 - i)
        cs2 = new LazyUTF8Sequence(buf2, i, len2 - i)
     }

      val cr = DefaultCollator.get.compare(cs1, cs2)
      if (cr != 0) {
        cr
      } else {
        // Collator deems them equal, but they aren't binary equal; for total ordering decide by binary sort order
        Integer.compare(buf1.getUnsignedByte(buf1.readerIndex + first), buf2.getUnsignedByte(buf2.readerIndex + first))
      }
    }

    // Advance readerIndex to the ends of the objects
    buf1.skipBytes(len1)
    buf2.skipBytes(len2)
    result
  }

  // Decodes UTF-8 characters on demand from the underlying buffer. It's
  // optimized for decoding the next undecoded character, and returning the
  // last decoded character repeatedly.
  class LazyUTF8Sequence(buf: ByteBuf, start: Int, len: Int) extends CharSequence {
    private[this] val r = buf.readerIndex
    private[this] val end = start + len

    private[this] var bytePos = start
    private[this] var charPos = 0
    private[this] var lastChar: Char = '\u0000'
    private[this] var pendingLowSurrogate: Char = '\u0000'

    def charAt(index: Int): Char =
      if (index < 0) {
        throw new IndexOutOfBoundsException
      } else if (index == charPos - 1) {
        lastChar
      } else if (index == charPos) {
        memoizeNextChar
      } else {
        if (index < charPos) {
          // Reset from start. A reset for index 0 is cheap and happens if the
          // collator needs to go for case-sensitive pass. A reset for different
          // indices is linear in cost, but happens extremely rarely, only when
          // the first difference falls into ICU backwards unsafe set and the
          // collator has to backtrack few characters.
          charPos = 0
          bytePos = start
          lastChar = '\u0000'
          pendingLowSurrogate = '\u0000'
        }
        while (charPos < index) memoizeNextChar
        memoizeNextChar
      }

    private def memoizeNextChar: Char = {
      val c = nextChar
      lastChar = c
      c
    }

    // NB: This method presumes the UTF-8 sequence is valid and skips many UTF-8
    // checks (unexpected number of continuation characters, overlong encodings)
    private def nextChar: Char = {
      charPos += 1
      if (pendingLowSurrogate != '\u0000') {
        val c = pendingLowSurrogate
        pendingLowSurrogate = '\u0000'
        c
      } else if (bytePos == end) {
        // Because length() is just an approximation, return an ignorable
        // character when asked past the actual end instead of throwing
        // an IOOBE.
        '\u0000'
      } else {
        val c1 = buf.getByte(r + bytePos)
        if ((c1 & 0x80) == 0) {
          bytePos += 1
          (c1 & 0x7F).toChar
        } else if ((c1 & 0xE0) == 0xC0) {
          val c2 = buf.getByte(r + bytePos + 1)
          bytePos += 2
          ((c1 & 0x1F) << 6 | (c2 & 0x3F)).toChar
        } else if ((c1 & 0xF0) == 0xE0) {
          val c2 = buf.getByte(r + bytePos + 1)
          val c3 = buf.getByte(r + bytePos + 2)
          bytePos += 3
          ((c1 & 0x0F) << 12 | (c2 & 0x3F) << 6 | (c3 & 0x3F)).toChar
        } else if ((c1 & 0xF8) == 0xF0) {
          // A Unicode supplementary character (U+10000 to U+10FFFF)
          // We need to decode it into two UTF-16 values.
          val c2 = buf.getByte(r + bytePos + 1)
          val c3 = buf.getByte(r + bytePos + 2)
          val c4 = buf.getByte(r + bytePos + 3)
          bytePos += 4
          val u21 = ((c1 & 0x7) << 18 | (c2 & 0x3F) << 12 | (c3 & 0x3F) << 6 | (c4 & 0x3F)) - 0x10000
          pendingLowSurrogate = (0xDC00 | (u21 & 0x3FF)).toChar // low surrogate
          (0xD800 | ((u21 >> 10) & 0x3FF)).toChar // high surrogate
        } else {
          charPos -= 1 // undo increment
          // Invalid start-of-sequence byte.
          log.error(s"Invalid start-of-sequence byte ${(c1.toInt & 0xff).toHexString} buf: ${buf.toHexString} start: $start end: $end bytePos: $bytePos.")
          throw new CharacterCodingException()
        }
      }
    }

    // This will start with an upper bound and approach the real value as the
    // string is scanned. This strategy is safe for use with the ICU collators
    // and avoids the need to scan the string for its character length.
    def length(): Int = charPos + (end - bytePos) + (if (pendingLowSurrogate != '\u0000') 1 else 0)

    // Not used by the collator
    def subSequence(start: Int, end: Int) =
      throw new UnsupportedOperationException("LazyUTF8Sequence.subSequence")
  }

  // arrays sort lexically
  private def compareArrays(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf): Int = {
    val vl1 = ib1 & CBORParser.InfoMask
    val vl2 = ib2 & CBORParser.InfoMask

    val len1 = readInt(vl1, buf1)
    val len2 = readInt(vl2, buf2)

    val min = Math.min(len1, len2)
    var i = 0L
    while (i < min) {
      val cmp = compare0(buf1, buf2)
      if (cmp != 0) return cmp
      i += 1
    }
    JLong.compare(len1, len2)
  }

  // maps sort lexically
  private def compareMaps(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf): Int = {
    val vl1 = ib1 & CBORParser.InfoMask
    val vl2 = ib2 & CBORParser.InfoMask

    val len1 = readInt(vl1, buf1)
    val len2 = readInt(vl2, buf2)

    val min = Math.min(len1, len2)
    var i = 0L
    while (i < min * 2) {
      val cmp = compare0(buf1, buf2)
      if (cmp != 0) return cmp
      i += 1
    }
    JLong.compare(len1, len2)
  }

  private def compareTagged(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf) = {
    val vl1 = ib1 & CBORParser.InfoMask
    val vl2 = ib2 & CBORParser.InfoMask

    val tag1 = readInt(vl1, buf1)
    val tag2 = readInt(vl2, buf2)

    JLong.compare(tag1, tag2) match {
      case 0 =>
        if (tag1 == TypeInfos.EpochTimestampTag) {
          compareTimestamps(buf1, buf2)
        } else if (tag1 == CBOR.ReverseTag) {
          compare0(buf2, buf1)
        } else {
          compare0(buf1, buf2)
        }
      case cmp => cmp
    }
  }

  private def compareMisc(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf) = {
    val vl1 = ib1 & CBORParser.InfoMask
    val vl2 = ib2 & CBORParser.InfoMask

    // nil values sort last

    vl1 match {
      case TypeInfos.NilInfo =>
        vl2 match {
          case TypeInfos.NilInfo => 0 // nil v. nil
          case _                 => 1 // nil v. non-nil
        }

      case TypeInfos.FloatInfo =>
        vl2 match {
          case TypeInfos.NilInfo   => -1 // non-nil v. nil
          case TypeInfos.TrueInfo | TypeInfos.FalseInfo => -1
          case TypeInfos.FloatInfo => JFloat.compare(buf1.readFloat, buf2.readFloat)
          case _                   => Integer.compare(vl1, vl2) // other 'misc' values
        }

      case TypeInfos.DoubleInfo =>
        vl2 match {
          case TypeInfos.NilInfo    => -1 // non-nil v. nil
          case TypeInfos.TrueInfo | TypeInfos.FalseInfo => -1
          case TypeInfos.DoubleInfo => JDouble.compare(buf1.readDouble, buf2.readDouble)
          case _                    => Integer.compare(vl1, vl2) // other 'misc' values
        }

      case _ =>
        vl2 match {
          case TypeInfos.NilInfo => -1 // non-nil v. nil
          case TypeInfos.DoubleInfo | TypeInfos.FloatInfo => +1
          case _                 => Integer.compare(vl1, vl2) // other 'misc' values
        }
    }
  }

  private def compareIntNumber(ib1: Int, buf1: ByteBuf, ib2: Int, buf2: ByteBuf) = {
    val vl2 = ib2 & CBORParser.InfoMask

    if (vl2 == TypeInfos.FloatInfo) {
      JFloat.compare(readInt(ib1, buf1).toFloat, buf2.readFloat)
    } else {
      JDouble.compare(readInt(ib1, buf1).toDouble, buf2.readDouble)
    }
  }

  private[this] val TwoSlotArray = 0x82

  private def compareTimestamps(buf1: ByteBuf, buf2: ByteBuf) = {
    val ib1 = buf1.readUnsignedByte
    val ib2 = buf2.readUnsignedByte

    var sec1 = 0L
    var nan1 = 0L

    if (ib1 == TwoSlotArray) {
      sec1 = readInt(buf1.readUnsignedByte, buf1)
      nan1 = readInt(buf1.readUnsignedByte, buf1)
    } else {
      sec1 = readInt(ib1, buf1)
    }

    var sec2 = 0L
    var nan2 = 0L

    if (ib2 == TwoSlotArray) {
      sec2 = readInt(buf2.readUnsignedByte, buf2)
      nan2 = readInt(buf2.readUnsignedByte, buf2)
    } else {
      sec2 = readInt(ib2, buf2)
    }

    JLong.compare(sec1, sec2) match {
      case 0   => JLong.compare(nan1, nan2)
      case cmp => cmp
    }
  }
}
