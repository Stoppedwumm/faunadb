package fauna.util.router

import io.netty.handler.codec.http.HttpMethod
import scala.annotation.unused

trait RoutingException extends Exception
case object InvalidPathArg extends RoutingException {
  override def getMessage = "Invalid path argument."
}

sealed trait Param[A] {
  def apply(str: String): A
}

case class LongParam(min: Long, max: Long) extends Param[Long] {
  def apply(str: String) = {
    str.toLongOption
      .filter { l => l >= min && l <= max }
      .getOrElse(throw InvalidPathArg)
  }
}

case object StringParam extends Param[String] {
  def apply(str: String) = if (str.nonEmpty) str else throw InvalidPathArg
}

class NoParamBuilder[Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res]) {
  def /(segment: String) = new NoParamBuilder(method, path+"/"+segment, router)

  def /[A](p1: Param[A]) = new OneParamBuilder(method, path+"/{param}", router, p1)

  def /(func: Req => Res) = {
    def handler(@unused params: Seq[String], req: Req) = func(req)

    router.addRoute(method, path, handler)
  }
}

class OneParamBuilder[A, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A]) {
  def /(segment: String) = new OneParamBuilder(method, path+"/"+segment, router, p1)

  def /[B](p2: Param[B]) = new TwoParamBuilder(method, path+"/{param}", router, p1, p2)

  def /(func: (A, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a) => func(p1(a), req)
      case _      => throw new AssertionError(s"Expected 1 arg, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class TwoParamBuilder[A, B, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B]) {
  def /(segment: String) = new TwoParamBuilder(method, path+"/"+segment, router, p1, p2)

  def /[C](p3: Param[C]) = new ThreeParamBuilder(method, path+"/{param}", router, p1, p2, p3)

  def /(func: (A, B, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b) => func(p1(a), p2(b), req)
      case _         => throw new AssertionError(s"Expected 2 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class ThreeParamBuilder[A, B, C, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B], p3: Param[C]) {
  def /(segment: String) = new ThreeParamBuilder(method, path+"/"+segment, router, p1, p2, p3)

  def /[D](p4: Param[D]) = new FourParamBuilder(method, path+"/{param}", router, p1, p2, p3, p4)

  def /(func: (A, B, C, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b, c) => func(p1(a), p2(b), p3(c), req)
      case _            => throw new AssertionError(s"Expected 3 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class FourParamBuilder[A, B, C, D, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B], p3: Param[C], p4: Param[D]) {
  def /(segment: String) = new FourParamBuilder(method, path+"/"+segment, router, p1, p2, p3, p4)

  def /[E](p5: Param[E]) = new FiveParamBuilder(method, path+"/{param}", router, p1, p2, p3, p4, p5)

  def /(func: (A, B, C, D, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b, c, d) => func(p1(a), p2(b), p3(c), p4(d), req)
      case _               => throw new AssertionError(s"Expected 4 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class FiveParamBuilder[A, B, C, D, E, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B], p3: Param[C], p4: Param[D], p5: Param[E]) {
  def /(segment: String) = new FiveParamBuilder(method, path+"/"+segment, router, p1, p2, p3, p4, p5)

  def /[F](p6: Param[F]) = new SixParamBuilder(method, path+"/{param}", router, p1, p2, p3, p4, p5, p6)

  def /(func: (A, B, C, D, E, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b, c, d, e) => func(p1(a), p2(b), p3(c), p4(d), p5(e), req)
      case _                  => throw new AssertionError(s"Expected 5 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class SixParamBuilder[A, B, C, D, E, F, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B], p3: Param[C], p4: Param[D], p5: Param[E], p6: Param[F]) {
  def /(segment: String) = new SixParamBuilder(method, path+"/"+segment, router, p1, p2, p3, p4, p5, p6)

  def /[G](p7: Param[G]) = new SevenParamBuilder(method, path+"/{param}", router, p1, p2, p3, p4, p5, p6, p7)

  def /(func: (A, B, C, D, E, F, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b, c, d, e, f) => func(p1(a), p2(b), p3(c), p4(d), p5(e), p6(f), req)
      case _                  => throw new AssertionError(s"Expected 6 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}

class SevenParamBuilder[A, B, C, D, E, F, G, Req, Res](method: HttpMethod, path: String, router: Router[(Seq[String], Req) => Res], p1: Param[A], p2: Param[B], p3: Param[C], p4: Param[D], p5: Param[E], p6: Param[F], p7: Param[G]) {
  def /(segment: String) = new SevenParamBuilder(method, path+"/"+segment, router, p1, p2, p3, p4, p5, p6, p7)

  // def /[H](p8: Param[H]) = new EightParamBuilder(method, path+"/{param}", router, p1, p2, p3, p4, p5, p6, p7, p8)

  def /(func: (A, B, C, D, E, F, G, Req) => Res) = {
    def handler(params: Seq[String], req: Req) = params match {
      case Seq(a, b, c, d, e, f, g) => func(p1(a), p2(b), p3(c), p4(d), p5(e), p6(f), p7(g), req)
      case _                  => throw new AssertionError(s"Expected 7 args, got ${params.size}")
    }

    router.addRoute(method, path, handler)
  }
}
