package fauna.prop

import fauna.lang.{ Monad, Timestamp }
import fauna.lang.clocks.Clock
import java.security.{ KeyPair, KeyPairGenerator }
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{ Calendar, GregorianCalendar, UUID }
import scala.collection.mutable.{ Set => MSet }
import scala.concurrent.duration._
import scala.math.abs
import scala.util.matching.Regex
import scala.util.Random

case class PropConfig(
  maxDiscarded: Int = 300,
  minSuccessful: Int = 10,
  seed: Long = (new Random).nextLong()
) {

  val rand = new Random(seed)
}

abstract class Prop[+T] { self =>
  def run(conf: PropConfig): Option[T]

  def test(implicit conf: PropConfig) =
    for (_ <- 0 until conf.minSuccessful) sample(conf)

  def sample(implicit conf: PropConfig): T = {
    var discarded = 0

    while (discarded <= conf.maxDiscarded) {
      run(conf) match {
        case Some(x) => return x
        case _       => ()
      }

      discarded += 1
    }

    sys.error(s"Too many discarded values")
  }

  def map[R](f: T => R): Prop[R] = new Prop[R] {
    def run(conf: PropConfig) = self.run(conf) map f
  }

  // pattern-match is necessary since for-comprehensions without yield map every
  // level to foreach, unlike map/flatMap.
  def foreach[U](f: T => U): Prop[Unit] = flatMap { t =>
    f(t) match {
      case p: Prop[_] =>
        p map { _ =>
          ()
        }
      case _ => Prop.const(())
    }
  }

  def flatMap[R](f: T => Prop[R]): Prop[R] = new Prop[R] {
    def run(conf: PropConfig) = self.run(conf) flatMap { f(_).run(conf) }
  }

  def filter(f: T => Boolean) = new Prop[T] {
    def run(conf: PropConfig) = self.run(conf) filter f
  }

  def withFilter(f: T => Boolean) = filter(f)

  def times(x: Int): Prop[Seq[T]] =
    if (x == 0) {
      Prop.const(Nil)
    } else {
      new Prop[Seq[T]] {
        def run(conf: PropConfig): Option[Seq[T]] = {
          var count = x
          val ret = Seq.newBuilder[T]
          while (count > 0) {
            self.run(conf) match {
              case Some(v) =>
                ret += v
                count -= 1
              case None =>
                return None
            }
          }
          Some(ret.result())
        }
      }
    }

  def *(x: Int) = this times x
}

object Prop {

  def apply[T](t: PropConfig => T): Prop[T] =
    new Prop[T] { def run(conf: PropConfig) = Some(t(conf)) }

  def unique[T](prop: Prop[T], seen: MSet[T] = MSet.empty[T]): Prop[T] =
    new Prop[T] {
      def run(conf: PropConfig) = {
        def run0(discarded: Int = 0): Option[T] = {
          prop.run(conf) flatMap { value =>
            val wasSeen = seen.synchronized {
              val wasSeen = seen contains value
              if (!wasSeen) seen += value
              wasSeen
            }
            if (!wasSeen) {
              Some(value)
            } else if (discarded <= conf.maxDiscarded) {
              run0(discarded + 1)
            } else {
              None
            }
          }
        }
        run0()
      }
    }

  def choose[T](ts: Iterable[T]) = {
    val seq = ts.toIndexedSeq

    int(seq.size) map { seq(_) }
  }

  def choose[T](t1: T, t2: T, ts: T*): Prop[T] = choose(t1 +: t2 +: ts)

  def shuffle[T](seq: Seq[T]): Prop[Seq[T]] = Prop { _.rand.shuffle(seq) }

  def const[T](t: => T) = Prop { _ =>
    t
  }

  val int: Prop[Int] = Prop { _.rand.nextInt() }

  // [0, limit)
  def int(limit: Int): Prop[Int] = Prop { _.rand.nextInt(limit) }

  def int(r: Range): Prop[Int] = Prop { _.rand.nextInt(r.length) + r.start }

  val positiveInt: Prop[Int] = int(Int.MaxValue).filter { _ != 0 }
  val negativeInt: Prop[Int] = positiveInt.map { _ * -1 }

  val long = Prop { p =>
    abs(p.rand.nextLong())
  }

  def long(limit: Long): Prop[Long] = Prop.long map { _ % limit }

  val float = Prop { conf =>
    conf.rand.nextFloat() * conf.rand.nextInt()
  }

  val double = Prop { conf =>
    conf.rand.nextDouble() * conf.rand.nextInt()
  }

  val boolean = Prop { _.rand.nextBoolean() }

  def either[S, T](a: Prop[S], b: Prop[T]): Prop[Either[S, T]] =
    Prop { p =>
      if (p.rand.nextBoolean()) {
        Left(a.sample(p))
      } else {
        Right(b.sample(p))
      }
    }

  private def isSurrogate(c: Char) =
    (c >= 0xd800 && c <= 0xdbff) || (c >= 0xdc00 && c <= 0xdfff)

  val char = Prop { cfg =>
    var c = cfg.rand.nextInt().toChar
    while (isSurrogate(c)) c = cfg.rand.nextInt().toChar; c
  }

  def opt[T](t: => T): Prop[Option[T]] = boolean map { if (_) Some(t) else None }

  def string(
    minSize: Int,
    maxSize: Int,
    chars: Iterable[Char]): Prop[String] =
    string(minSize, maxSize, Prop.choose(chars))

  def string(
    minSize: Int = 0,
    maxSize: Int = 2000,
    chars: Prop[Char] = char
  ): Prop[String] =
    Prop { conf =>
      val length = Prop.int(minSize to maxSize).sample(conf)
      (0 until length).iterator.map { _ => chars.sample(conf) }.mkString
    }

  val string: Prop[String] = string()

  private val numChars: Seq[Char] = ('0' to '9')
  private val alphaLowerChars: Seq[Char] = ('a' to 'z')
  private val alphaUpperChars: Seq[Char] = ('A' to 'Z')
  private val alphaChars: Seq[Char] = alphaLowerChars ++ alphaUpperChars
  private val alphaNumChars: Seq[Char] = alphaChars ++ numChars
  private val hexChars: Seq[Char] = ('A' to 'F') ++ numChars
  private val identChars: Seq[Char] = alphaNumChars :+ '_'
  private val identStartChars: Seq[Char] = alphaChars :+ '_'

  def numericString(minSize: Int = 0, maxSize: Int = 32): Prop[String] =
    string(minSize, maxSize, numChars)

  def alphaString(minSize: Int = 0, maxSize: Int = 32): Prop[String] =
    string(minSize, maxSize, alphaChars)

  def alphaNumString(minSize: Int = 0, maxSize: Int = 32): Prop[String] =
    string(minSize, maxSize, alphaNumChars)

  def hexString(minSize: Int = 0, maxSize: Int = 32): Prop[String] =
    string(minSize, maxSize, hexChars)

  def identifierString(minSize: Int = 1, maxSize: Int = 32): Prop[String] =
    for {
      s <- choose(identStartChars)
      e <- string(minSize - 1, maxSize - 1, identChars)
    } yield s +: e

  val aSafeURL: Prop[String] = aURL(Prop.const(true))

  val aURL: Prop[String] = aURL(boolean)

  def aURL(prot: Prop[Boolean]): Prop[String] = for {
    proto    <- prot map { if (_) "https" else "http" }
    domain   <- alphaNumString(3, 50)
    tld      <- alphaString(3, 5)
    levelCnt <- int(6)
    levels   <- alphaNumString().times(levelCnt)
  } yield s"${proto}://${domain}.${tld}/${levels.mkString("/")}"

  val aJwksUri: Prop[String] = for {
    name <- hexString(minSize = 1)
  } yield s"https://$name.auth0.com/.well-known/jwks.json"

  val anEmailAddress: Prop[String] = for {
    local  <- alphaNumString(1, 32)
    domain <- alphaNumString(1, 32)
    tld    <- alphaString(3, 3)
  } yield s"$local@$domain.$tld"

  val date = for {
    gc <- Prop { _ =>
      new GregorianCalendar()
    }
    year <- int(1970 until 2038)
    _ = gc.set(Calendar.YEAR, year)
    dayOfYear <- int(1 to gc.getActualMaximum(Calendar.DAY_OF_YEAR))
    _ = gc.set(Calendar.DAY_OF_YEAR, dayOfYear)
  } yield gc.getTime()

  def timestamp(limit: Timestamp = Clock.time): Prop[Timestamp] = for {
    millis <- Prop.long(limit.millis)
  } yield Timestamp.ofMillis(millis)

  def timestampAfter(
    ts: Timestamp,
    limit: Timestamp = Clock.time): Prop[Timestamp] = {
    require(limit > ts, "limit must be greater than given timestamp")
    Prop.long(limit.difference(ts).toMillis + 1) map { addition =>
      ts + addition.micros
    }
  }

  def isoTime(limit: Timestamp = Clock.time): Prop[String] =
    timestamp(limit) map { ts => DateTimeFormatter.ISO_INSTANT.format(ts.toInstant) }

  val isoDate = for {
    fmt <- Prop { _ =>
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    }
    d <- date
  } yield fmt.format(d)

  def normalizedStream(mean: Double, stddev: Double) = Prop { conf =>
    val variance = stddev * stddev
    LazyList.continually(variance * conf.rand.nextGaussian() + mean)
  }

  val uuid: Prop[UUID] = {
    val guid = for {
      data1  <- hexString(8, 8)
      data2  <- hexString(4, 4)
      data3  <- hexString(4, 4)
      data4a <- hexString(4, 4)
      data4b <- hexString(12, 12)
    } yield UUID.fromString(s"${data1}-${data2}-${data3}-${data4a}-${data4b}")
    Prop.unique(guid)
  }

  val regex: Prop[Regex] = Prop.choose(
    ".*".r,
    ".".r,
    "\\.".r,
    "\\d".r,
    "\\D".r,
    "\\w".r,
    "\\W".r,
    "\\s".r,
    "\\S".r,
    "\\z".r,
    "\\Z".r
  )

  def aRSAKeyPair(keySize: Int = 2048): Prop[KeyPair] = {
    val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(keySize)
    Prop.const(kpg.generateKeyPair())
  }

  implicit object MonadInstance extends Monad[Prop] {
    def pure[A](a: A) = Prop.const(a)
    def map[A, B](m: Prop[A])(f: A => B) = m map f
    def flatMap[A, B](m: Prop[A])(f: A => Prop[B]) = m flatMap f

    def accumulate[A, B](ms: Iterable[Prop[A]], seed: B)(f: (B, A) => B) =
      ms.foldLeft(Prop.const(seed)) { case (acc, next) =>
        for {
          a <- acc
          n <- next
        } yield f(a, n)
      }
  }
}
