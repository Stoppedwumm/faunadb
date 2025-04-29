package fauna.net.test

import fauna.lang.clocks.TestClock
import fauna.lang.Timestamp
import fauna.net.security._
import fauna.prop.{ Prop, PropConfig }
import java.net.URL
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._

class JWKProviderSpec extends Spec {

  implicit val propConfig = PropConfig()

  def aJWK =
    for {
      keyPair <- Prop.aRSAKeyPair()
      kid     <- Prop.hexString()
    } yield { JWK.rsa(kid, keyPair) }

  val clock = new TestClock(Timestamp(Instant.now))

  val jwk = aJWK sample

  def successful(jwk: JWK, etag: ETag = None): Future[Option[(JWKS, ETag)]] =
    Future.successful(Some((JWKS(Seq(jwk)), etag)))

  "JWKSLoadingValue" - {
    "should load if the first time" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      val loadingValue = JWKSLoadingValue(jwksUrl) { (_, _, _) =>
        successful(jwk)
      }

      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk
    }

    "should not get again if loaded" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      var fut = successful(jwk)
      val loadingValue = JWKSLoadingValue(jwksUrl) { (_, _, _) =>
        fut
      }

      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      fut = Future.failed(new Exception)

      noException should be thrownBy {
        result(loadingValue.getJWK(jwk.kid)) shouldBe jwk
      }
    }

    "should fail if kid is unknown in the first time" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")
      val loadingValue = JWKSLoadingValue(jwksUrl) { (_, _, _) =>
        successful(jwk)
      }

      a[JWKNotFound] should be thrownBy {
        result(loadingValue.getJWK(Some("blah")))
      }
    }

    "should reload after loaded" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      var retJwk = jwk

      val loadingValue = JWKSLoadingValue(jwksUrl, clock = clock) { (_, _, _) =>
        successful(retJwk)
      }

      // load
      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      retJwk = aJWK.sample

      clock.advance(1.second)

      // force reload
      result(loadingValue.getJWK(retJwk.kid)) shouldBe retJwk
    }

    "should fail if kid is unknown after loaded" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      var retJwk = jwk

      val loadingValue = JWKSLoadingValue(jwksUrl) { (_, _, _) =>
        successful(retJwk)
      }

      // load
      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      retJwk = aJWK.sample

      // force reload
      a[JWKNotFound] should be thrownBy {
        result(loadingValue.getJWK(Some("blah")))
      }
    }

    "JWKS should be in place while new one is downloaded" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      var retJwk = successful(jwk, Some("ETagOld"))

      val loadingValue = JWKSLoadingValue(jwksUrl, None, 1000.micros, clock) {
        (_, _, _) =>
          retJwk
      }

      // load
      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      val p = Promise[Option[(JWKS, ETag)]]()

      retJwk = p.future

      val newJwk = aJWK.sample

      // timestamp before reloading
      val ts0 = clock.micros

      clock.advance(1.second)

      // kid is different, force reload
      val fut = loadingValue.getJWK(newJwk.kid)

      // assert old jwks is still in place
      loadingValue.getValue() shouldBe Right(
        Some((JWKS(List(jwk)), ts0, Some("ETagOld"))))

      // finish download
      p.success(Some((JWKS(List(newJwk)), Some("ETagNew"))))

      result(fut) shouldBe newJwk

      loadingValue.getValue() shouldBe Right(
        Some((JWKS(List(newJwk)), clock.micros, Some("ETagNew"))))
    }

    "should limit outbound requests" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      var asserter = () => {
        successful(jwk, Some("ETagOld"))
      }

      val loadingValue = JWKSLoadingValue(jwksUrl, None, 1000.micros, clock) {
        (_, _, _) =>
          asserter()
      }

      // load
      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      // timestamp before reloading
      val ts0 = clock.micros

      clock.advance(100.micros)

      asserter = () => {
        fail("should not be called")
      }

      // kid is different, force reload
      val fut = loadingValue.getJWK(Some("blah"))

      // assert old jwks is still in place
      loadingValue.getValue() shouldBe Right(
        Some((JWKS(List(jwk)), ts0, Some("ETagOld"))))

      a[JWKNotFound] should be thrownBy {
        result(fut)
      }

      loadingValue.getValue() shouldBe Right(
        Some((JWKS(List(jwk)), ts0, Some("ETagOld"))))
    }

    "should persist ETag headers" in {
      val jwksUrl = new URL("https://dev.auth0.com/something")

      val loadingValue = JWKSLoadingValue(jwksUrl, clock = clock) { (_, _, _) =>
        successful(jwk, Some("ETag"))
      }

      loadingValue.getValue() shouldBe Right(None)

      result(loadingValue.getJWK(jwk.kid)) shouldBe jwk

      loadingValue.getValue() shouldBe Right(
        Some((JWKS(List(jwk)), clock.micros, Some("ETag"))))
    }
  }

  "JWKUrlProvider" - {
    "work" in {
      val provider =
        new JWKUrlProvider(List("127\\.0\\.0\\.1".r), Some(DefaultSSL)) {

          override def jwksGet(
            url: URL,
            etag: Option[String],
            ssl: Option[SSLConfig]) = {
            ssl shouldBe Some(DefaultSSL)

            successful(jwk)
          }
        }

      result(
        provider
          .getJWK(new URL("https://dev.auth0.com/something"), jwk.kid)) shouldBe jwk
    }

    "should not consider two URLs equivalent if they resolve to the same IP" in {
      val jwk1 = aJWK sample
      val jwksUrl1 = new URL("http://127.0.0.1")

      val jwk2 = aJWK sample
      val jwksUrl2 = new URL("http://localhost")

      // The URLs resolve to the same IP therefore they are considered equals by
      // `java.net.URL`
      jwksUrl1 shouldBe jwksUrl2

      val fetched = new AtomicInteger(0)
      val provider =
        new JWKUrlProvider(List("127\\.0\\.0\\.1".r), Some(NoSSL)) {

          override def jwksGet(
            url: URL,
            etag: Option[String],
            ssl: Option[SSLConfig]) = {
            fetched.incrementAndGet()
            if (url.toString == jwksUrl1.toString) {
              successful(jwk1)
            } else {
              successful(jwk2)
            }
          }
        }

      result(
        provider
          .getJWK(jwksUrl1, jwk1.kid)) shouldBe jwk1
      fetched.get() shouldBe 1
      result(
        provider
          .getJWK(jwksUrl1, jwk1.kid)) shouldBe jwk1
      // second call goes to the cache
      fetched.get() shouldBe 1

      result(
        provider
          .getJWK(jwksUrl2, jwk2.kid)) shouldBe jwk2
      fetched.get() shouldBe 2
      result(
        provider
          .getJWK(jwksUrl2, jwk2.kid)) shouldBe jwk2
      // second call goes to the cache
      fetched.get() shouldBe 2
    }

    "provider is safe" in {
      val provider =
        new JWKUrlProvider(List("127\\.0\\.0\\.1".r), Some(DefaultSSL)) {

          override def jwksGet(
            url: URL,
            etag: Option[String],
            ssl: Option[SSLConfig]) = {
            ssl shouldBe Some(NoSSL)

            successful(jwk)
          }
        }

      result(
        provider
          .getJWK(new URL("https://127.0.0.1/something"), jwk.kid)) shouldBe jwk
    }

    "expire cache" in {
      var count = 0

      val provider = new JWKUrlProvider(
        List("127\\.0\\.0\\.1".r),
        Some(DefaultSSL),
        expireRate = 100.milliseconds,
        refreshRate = 1.minute) {

        override def jwksGet(
          url: URL,
          etag: Option[String],
          ssl: Option[SSLConfig]) = {
          ssl shouldBe Some(NoSSL)

          count += 1

          successful(jwk)
        }
      }

      result(
        provider
          .getJWK(new URL("https://127.0.0.1/something"), jwk.kid)) shouldBe jwk

      count shouldBe 1

      Thread.sleep(500)

      result(
        provider
          .getJWK(new URL("https://127.0.0.1/something"), jwk.kid)) shouldBe jwk

      count shouldBe 2
    }

    "auto refresh" in {
      var count = 0

      val provider = new JWKUrlProvider(
        List("127\\.0\\.0\\.1".r),
        Some(DefaultSSL),
        expireRate = 1.minute,
        refreshRate = 100.milliseconds) {

        override def jwksGet(
          url: URL,
          etag: Option[String],
          ssl: Option[SSLConfig]) = {
          ssl shouldBe Some(NoSSL)

          count += 1

          successful(jwk)
        }
      }

      result(
        provider
          .getJWK(new URL("https://127.0.0.1/something"), jwk.kid)) shouldBe jwk

      count shouldBe 1

      Thread.sleep(500)

      result(
        provider
          .getJWK(new URL("https://127.0.0.1/something"), jwk.kid)) shouldBe jwk

      count shouldBe 2
    }
  }

  private def result[T](f: Future[T]): T =
    Await.result(f, 1.minute)
}
