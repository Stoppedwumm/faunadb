package fauna.net

import scala.concurrent.Future

package object http {
  type HttpHandlerF = (HttpRequestChannelInfo, HttpRequest) => Future[HttpResponse]
  type HttpExceptionHandlerF = (HttpRequestChannelInfo, Throwable) => Future[HttpResponse]
}
