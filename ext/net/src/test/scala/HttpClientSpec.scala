package fauna.net.test

import fauna.net.http._
import fauna.net.security._

class HttpClientSpec extends Spec {
  "HttpClient" - {
    "infers config based on scheme" in {
      HttpClient("google.com").port should equal (80)
      HttpClient("http://google.com").port should equal (80)
      HttpClient("https://google.com").port should equal (443)

      HttpClient("google.com").ssl should equal (NoSSL)
      HttpClient("http://google.com").ssl should equal (NoSSL)
      HttpClient("https://google.com").ssl should equal (DefaultSSL)
    }

    "derives port from url" in {
      HttpClient("google.com:8080").port should equal (8080)
      HttpClient("http://google.com:8080").port should equal (8080)
      HttpClient("https://google.com:8080").port should equal (8080)
    }

    "manual config overrides url-derived" in {
      HttpClient("google.com:8080", port = 3000).port should equal (3000)
      HttpClient("http://google.com", ssl = Some(DefaultSSL)).ssl should equal (DefaultSSL)
    }
  }
}
