package fauna.net.test

import fauna.net.Network
import fauna.net.http._
import fauna.net.netty.ChannelClosedException
import fauna.net.security._
import io.netty.handler.codec.http.HttpResponseStatus
import javax.net.ssl.SSLException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await

class SSLSpec extends Spec {

  val pw = Password("secret")

  val echoHandler: HttpHandlerF = { (_, req) =>
    HttpServer.discard(req) map { _ =>
      HttpResponse(HttpResponseStatus.OK, Body("ok", "text/plain"))
    }
  }

  "SSL" - {
    "can verify a CA-signed cert with default config" in {
      val cl = HttpClient("https://www.google.com")
      val fut = cl.get("/") flatMap { res =>
        res.code should equal(200)
        res.body.maybeRelease()
      }
      Await.ready(fut, 30.seconds)
    }

    "does not trust CA-signed cert with configured trust" in {
      val trust = TrustSource(testPEMFile("root_trust.pem"))
      val cl = HttpClient("https://google.com", ssl = Some(SSL(trust)))
      a[SSLException] should be thrownBy Await.result(cl.get("/"), Duration.Inf)
    }

    "does not trust a revoked cert" in {
      val trust = TrustSource(testPEMFile("root_trust.pem"))

      val goodKey = KeySource(testPEMFile("encrypted_server_cert_chain1.pem"), pw)
      val badKey = KeySource(testPEMFile("revoked_server_cert_chain.pem"), pw)

      val goodPort = findFreePort()
      val badPort = findFreePort()

      val good =
        HttpServer(Network.address(goodPort), ssl = SSL(goodKey, false))(echoHandler)
      val bad =
        HttpServer(Network.address(badPort), ssl = SSL(badKey, false))(echoHandler)

      good.start()
      bad.start()

      val res = Await.result(
        HttpClient(s"https://localhost:$goodPort", ssl = Some(SSL(trust))).get(""),
        Duration.Inf)
      res.code should equal(200)
      res.body.maybeRelease()

      a[SSLException] should be thrownBy Await.result(
        HttpClient(s"https://localhost:$badPort", ssl = Some(SSL(trust))).get(""),
        Duration.Inf)

      good.stop()
      bad.stop()
    }

    "shared public/private key" - {
      val key = testPEMFile("private_key1.pem")

      testServerAuth(KeySource(key), TrustSource(key))
      testPrivateServerAuth(KeySource(key))
      testMutualAuth(
        KeySource(key),
        TrustSource(key),
        KeySource(key),
        TrustSource(key))
    }

    "shared password-protected public/private key" - {
      val key = testPEMFile("encrypted_private_key1.pem")

      testServerAuth(KeySource(key, pw), TrustSource(key, pw))
      testPrivateServerAuth(KeySource(key, pw))
      testMutualAuth(
        KeySource(key, pw),
        TrustSource(key, pw),
        KeySource(key, pw),
        TrustSource(key, pw))
    }

    "private key and separate public key" - {
      val key = testPEMFile("encrypted_private_key1.pem")
      val trust = testPEMFile("public_key1.pem")

      testServerAuth(KeySource(key, pw), TrustSource(trust))
      testPrivateServerAuth(KeySource(key, pw))
      testMutualAuth(
        KeySource(key, pw),
        TrustSource(trust),
        KeySource(key, pw),
        TrustSource(trust))
    }

    "individual private keys and list of public keys" - {
      val key1 = testPEMFile("encrypted_private_key1.pem")
      val key2 = testPEMFile("encrypted_private_key2.pem")
      val trust = testPEMFile("all_public_keys.pem")

      testServerAuth(KeySource(key1, pw), TrustSource(trust))
      testPrivateServerAuth(KeySource(key1, pw))
      testMutualAuth(
        KeySource(key1, pw),
        TrustSource(trust),
        KeySource(key2, pw),
        TrustSource(trust))
    }

    "trusted root cert and individual cert chains" - {
      val key1 = testPEMFile("encrypted_server_cert_chain1.pem")
      val key2 = testPEMFile("encrypted_client_cert_chain2.pem")
      val trust = testPEMFile("root_trust.pem")

      testServerAuth(KeySource(key1, pw), TrustSource(trust))
      testPrivateServerAuth(KeySource(key1, pw))
      testMutualAuth(
        KeySource(key1, pw),
        TrustSource(trust),
        KeySource(key2, pw),
        TrustSource(trust))
    }

    def testServerAuth(key: KeySource, trust: TrustSource) =
      "can be used for server auth" in {
        val port = findFreePort()
        val server =
          HttpServer(Network.address(port), ssl = SSL(key, false))(echoHandler)
        server.start()

        // Correct config
        val good = HttpClient(s"https://localhost:$port", ssl = Some(SSL(trust)))
        val res = Await.result(good.get(""), Duration.Inf)
        res.code should equal(200)
        res.body.maybeRelease()

        // Trust not configured. Client rejects server.
        val bad1 = HttpClient(s"https://localhost:$port")
        a[SSLException] should be thrownBy Await.result(bad1.get(""), Duration.Inf)

        server.stop()
      }

    def testPrivateServerAuth(key: KeySource) =
      "can be used for private server auth" in {
        val ssl = SSL(key, true)
        val port = findFreePort()
        val server = HttpServer(Network.address(port), ssl = ssl)(echoHandler)
        server.start()

        // Correct config
        val good = HttpClient(s"https://localhost:$port", ssl = Some(ssl))
        val res = Await.result(good.get(""), Duration.Inf)
        res.code should equal(200)
        res.body.maybeRelease()

        // Trust not configured. Client rejects server.
        val bad1 = HttpClient(s"https://localhost:$port")
        a[SSLException] should be thrownBy Await.result(bad1.get(""), Duration.Inf)

        server.stop()
      }

    def testMutualAuth(
      sKey: KeySource,
      sTrust: TrustSource,
      cKey: KeySource,
      cTrust: TrustSource) =
      "can be used for client auth" in {
        val port = findFreePort()
        val server =
          HttpServer(Network.address(port), ssl = SSL(sKey, sTrust, false))(
            echoHandler)
        server.start()

        val good = HttpClient(
          s"https://localhost:$port",
          ssl = Some(SSL(cKey, cTrust, false)))
        val res = Await.result(good.get(""), Duration.Inf)
        res.code should equal(200)
        res.body.maybeRelease()

        // Client key and trust not configured. Client rejects server.
        val bad1 = HttpClient(s"https://localhost:$port")
        a[SSLException] should be thrownBy Await.result(bad1.get(""), Duration.Inf)

        // Client trust not configured. Client rejects server.
        val bad2 =
          HttpClient(s"https://localhost:$port", ssl = Some(SSL(cKey, false)))
        a[SSLException] should be thrownBy Await.result(bad2.get(""), Duration.Inf)

        // Client key not configured. Server rejects client.
        val bad3 = HttpClient(s"https://localhost:$port", ssl = Some(SSL(cTrust)))
        val ex3 = intercept[Exception] { Await.result(bad3.get(""), Duration.Inf) }
        assert(
          ex3.isInstanceOf[SSLException] || ex3.isInstanceOf[ChannelClosedException])

        server.stop()
      }
  }
}
