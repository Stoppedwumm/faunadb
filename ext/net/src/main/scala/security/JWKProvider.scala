package fauna.net.security

import com.github.benmanes.caffeine.cache._
import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.lang.clocks.Clock
import fauna.logging.ExceptionLogging
import fauna.net.http.{ HTTPHeaders, HttpClient }
import java.net.{ ConnectException, URL }
import java.time.Instant
import java.util.concurrent.{ CompletableFuture, Executor }
import java.util.concurrent.atomic.{ AtomicInteger, AtomicReference }
import org.github.jamm.MemoryMeter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ Future, Promise }
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import scala.util.{ Failure, Success }
import scala.util.matching.Regex

object JWTValidator {
  def basicValidations(jwt: JWT, now: Instant): Boolean = {
    // maybe reserve for internal use with rootKey as the secret?
    if (jwt.getAlgorithm == "HS256" || jwt.getAlgorithm == "HS384" || jwt.getAlgorithm == "HS512") {
      return false
    }

    if (jwt.getExpiresAt.exists(exp => now.compareTo(exp) >= 0)) {
      return false
    }

    if (jwt.getNotBefore.exists(nb => now.compareTo(nb) <= 0)) {
      return false
    }

    true
  }
}

trait JWKProvider {
  def getJWK(jwksUrl: URL, kid: Option[String]): Future[JWK]
}

case class JWKSKeyValue(url: URL, ssl: Option[SSLConfig]) {
  val urlString: String = url.toString

  // Override `equals` and `hashCode` because we do not want to rely on the URL's implementations which
  // enforce that URLs with hosts resolving the same IP are considered equivalent.
  // The JDK's behaviour breaks our cache so we use `url.toString` instead.
  override def equals(obj: Any): Boolean = obj match {
    case other: JWKSKeyValue => urlString == other.urlString && ssl == other.ssl
    case _                   => false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + urlString.hashCode()
    result = prime * result + ssl.hashCode()
    result
  }
}

case class JWKSLoadingValue(
  url: URL,
  ssl: Option[SSLConfig] = None,
  downloadRate: FiniteDuration = JWKUrlProvider.DefaultDownloadRate,
  clock: Clock = Clock)(jwksGet: (URL, ETag, Option[SSLConfig]) => Future[Option[(JWKS, ETag)]]) {

  type L = Promise[JWKS]
  type R = Option[(JWKS, Long, ETag)]

  private[this] val value = new AtomicReference[Either[L, R]](Right(None))

  def getValue(): Either[L, R] = value.get()

  override def equals(obj: Any): Boolean = obj match {
    case other: JWKSLoadingValue => url == other.url
    case _                       => false
  }

  override def hashCode(): Int = url.hashCode()

  def reload(ignoreEtag: Boolean = false): Future[Unit] = {
    val etag = if (ignoreEtag) {
      None
    } else {
      value.get() match {
        case Right(Some((_, _, etag))) => etag
        case _                         => None
      }
    }

    jwksGet(url, etag, ssl) map {
      //new jwks, update
      case Some((jwks, newEtag)) =>
        val ts = clock.micros

        value.getAndSet(Right(Some((jwks, ts, newEtag)))) match {
          case Left(p) => p.success(jwks)
          case _       => ()
        }

      //not modified, ignore
      case None =>
        value.get() match {
          case Left(p) => p.failure(new JWKNotFound)
          case _       => ()
        }
    } transform {
      case Success(_)  =>
        Success(())

      case Failure(ex) =>
        value.get() match {
          case Left(p) => p.failure(ex)
          case _       => ()
        }

        Failure(ex)
    }
  }

  def getJWK(kid: Option[String]): Future[JWK] = value.get() match {
    //first time
    case n @ Right(None) =>
      val p = Promise[JWKS]()

      if (value.compareAndSet(n, Left(p))) {
        reload()

        p.future flatMap { jwks =>
          jwks.findJWK(kid).toFuture
        }
      } else {
        getJWK(kid)
      }

    //is loaded
    case Right(Some((jwks, ts, _))) =>
      jwks.findJWK(kid) match {
        //if kid is known, returns it
        case Some(jwk) =>
          Future.successful(jwk)

        //if kid is unknown, force reload, ignore etag to avoid server caching
        case None =>
          val now = clock.micros

          if (now - ts < downloadRate.toMicros) {
            Future.failed(new JWKNotFound)
          } else {
            reload(ignoreEtag = true) flatMap { _ =>
              value.get() match {
                case Right(Some((jwks, _, _))) => jwks.findJWK(kid).toFuture
                case _                         => Future.failed(new JWKNotFound)
              }
            }
          }
      }

    //is loading
    case Left(p) =>
      p.future flatMap { _ =>
        getJWK(kid)
      }
  }

  implicit class optionToFuture(jwk: Option[JWK]) {
    def toFuture: Future[JWK] = Future.fromTry(jwk.toRight(new JWKNotFound).toTry)
  }
}

case class JWKUrlProvider(
  safeAccessProviders: List[Regex],
  defaultSSL: Option[SSLConfig],
  maxSizeMB: Long = JWKUrlProvider.DefaultMaxSize,
  expireRate: FiniteDuration = JWKUrlProvider.DefaultExpireRate,
  refreshRate: FiniteDuration = JWKUrlProvider.DefaultRefreshRate,
  downloadRate: FiniteDuration = JWKUrlProvider.DefaultDownloadRate)
    extends JWKProvider {

  private[this] object Loader extends CacheLoader[JWKSKeyValue, JWKSLoadingValue] {
      override def load(key: JWKSKeyValue): JWKSLoadingValue = {
        JWKSLoadingValue(key.url, key.ssl, downloadRate)(jwksGet)
      }

      override def asyncReload(key: JWKSKeyValue, oldValue: JWKSLoadingValue, ex: Executor): CompletableFuture[JWKSLoadingValue] = {
        oldValue.reload().map(_ => oldValue).asJava.toCompletableFuture
      }
    }

  private[this] object Weigher extends Weigher[JWKSKeyValue, JWKSLoadingValue] {
    private val meter = MemoryMeter.builder()
      .omitSharedBufferOverhead()
      .withGuessing(MemoryMeter.Guess.BEST)
      .build()

    private val counter = new AtomicInteger(0)

    private var size: Int = 0

    override def weigh(key: JWKSKeyValue, value: JWKSLoadingValue): Int = {
      if (counter.getAndIncrement % JWKUrlProvider.WeightAccuracy == 0) {
        val valSize = value.getValue() match {
          case Right(Some((jwks, _, etag))) => meter.measureDeep(jwks) + 8 + meter.measure(etag)
          case Right(None)                  => 0
          case Left(_)                      => 0
        }

        size = (meter.measure(key) + valSize).toInt
      }

      size
    }
  }

  private[this] val noSSL = Some(NoSSL)

  private[this] val cache: LoadingCache[JWKSKeyValue, JWKSLoadingValue] =
    Caffeine.newBuilder
      .maximumWeight(maxSizeMB * 1024 * 1024)
      .expireAfterAccess(expireRate.length, expireRate.unit)
      .refreshAfterWrite(refreshRate.length, refreshRate.unit)
      .ticker(Ticker.systemTicker())
      .weigher(Weigher)
      .build(Loader)

  def jwksGet(url: URL, etag: ETag, ssl: Option[SSLConfig]): Future[Option[(JWKS, ETag)]] =
    JWKUrlProvider.getJWKS(url, etag, ssl)

  def getJWK(jwksUrl: URL, kid: Option[String]): Future[JWK] = {
    val isSafe = safeAccessProviders exists {
      _.matches(jwksUrl.getHost)
    }

    val ssl = if (isSafe) {
      noSSL
    } else {
      defaultSSL
    }

    cache.get(JWKSKeyValue(jwksUrl, ssl)).getJWK(kid)
  }
}

object JWKUrlProvider extends ExceptionLogging {
  val WeightAccuracy = 8
  val DefaultMaxSize = 16L
  val DefaultRefreshRate = 1.hour
  val DefaultExpireRate = 1.hour
  val DefaultDownloadRate = 500.milliseconds

  // represents the hosts safe to do requests without SSL, this is mainly used for testing purposes
  // to skip self signed certificates verification.
  val safeAccessProviders =
    Option(System.getProperty("fauna.safe.access.providers")).toList flatMap {
      prop =>
        prop.split(";") filterNot { _.trim.isEmpty } map { _.trim.r }
    }

  def getJWKS(jwksUrl: URL, etag: ETag, ssl: Option[SSLConfig] = None): Future[Option[(JWKS, ETag)]] = {
    val host = s"${jwksUrl.getProtocol}://${jwksUrl.getHost}"

    val headers = etag match {
      case Some(etag) => Seq(HTTPHeaders.Accept -> "application/json", HTTPHeaders.IfNoneMatch -> etag)
      case None       => Seq(HTTPHeaders.Accept -> "application/json")
    }

    val client = HttpClient(host, jwksUrl.getPort, headers = headers, ssl = ssl)

    val jwksF = client.get(jwksUrl.getPath) flatMap { response =>
      val etag = response.getHeader(HTTPHeaders.ETag)

      response.code match {
        case 200 =>
          response.body.data map { data =>
            JSON.tryParse[JSValue](data) match {
              case Success(js) => Some((JWKS(js), etag))
              case Failure(ex) => throw ex
            }
          }

        case 304 =>
          Future.successful(None)

        case code =>
          val error = new JWKSError(jwksUrl, s"HTTP code $code")
          logException(error)
          Future.failed(error)
      }
    }

    jwksF onComplete { _ => client.stop() }

    jwksF transform {
      case value @ Success(_) => value

      case Failure(ex: ConnectException) =>
        // Don't log connect() timeouts/refusals, but tell the caller.
        Failure(new JWKSError(jwksUrl, ex.getMessage))

      case Failure(ex) =>
        logException(ex)
        Failure(new JWKSError(jwksUrl, ex.getMessage))
    }
  }
}

