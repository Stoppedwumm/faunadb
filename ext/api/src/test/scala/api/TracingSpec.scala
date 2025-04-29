package fauna.api.test

import fauna.net.http.{ HTTPHeaders, NoBody }
import fauna.net.util.URIEncoding

class TracingSpec extends API21Spec {

  "tracing" - {
    test("works") {
      // Flags should indicate we aren't tracing
      api
        .query(Now(), rootKey)
        .headers
        .get(HTTPHeaders.TraceParent.toString) should endWith("-00")

      admin.post(
        path = "/admin/tracing/enable",
        body = NoBody,
        query = s"secret=${URIEncoding.encode(rootKey)}",
        token = rootKey) should respond (OK)

      // Flags should indicate we are tracing
      api
        .query(Now(), rootKey)
        .headers
        .get(HTTPHeaders.TraceParent.toString) should endWith("-01")

      admin.post(
        path = "/admin/tracing/disable",
        body = NoBody,
        query = s"secret=${URIEncoding.encode(rootKey)}",
        token = rootKey) should respond (OK)

      // Flags should indicate we aren't tracing
      api
        .query(Now(), rootKey)
        .headers
        .get(HTTPHeaders.TraceParent.toString) should endWith("-00")
    }

    test("allows clients with the right secret to set the flag") {
      val traceParent = "00-000000000000000050a5474ed8834769-5263798bc0583f83-01"
      // Flags should indicate we aren't tracing
      api
        .query(Now(), rootKey, Seq(HTTPHeaders.TraceParent -> traceParent))
        .headers
        .get(HTTPHeaders.TraceParent.toString) should endWith("-00")

      // Flags should indicate we are tracing
      api
        .query(
          Now(),
          rootKey,
          Seq(
            HTTPHeaders.TraceParent -> traceParent,
            HTTPHeaders.TraceSecret -> "randomsecretfortracing"
          ))
        .headers
        .get(HTTPHeaders.TraceParent.toString) should endWith("-01")
    }
  }
}
