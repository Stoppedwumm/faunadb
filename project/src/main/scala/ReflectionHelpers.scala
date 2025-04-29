package fauna.sbt

import language.implicitConversions

object ReflectionHelpers {
  case class WithManifest[T: Manifest](t: T) {
    val manifest = implicitly[Manifest[T]]
  }

  implicit def toWithManifest[T: Manifest](t: T) = WithManifest(t)

  def invokeStaticMethod(
    loader: ClassLoader,
    cls: String,
    method: String,
    args: WithManifest[_ <: AnyRef]*) = {
    try {
      val s = Class
        .forName(cls, true, loader)
        .getMethod(method, args.toArray map { _.manifest.runtimeClass }: _*)
        .invoke(null, args.toArray map { _.t }: _*)

      ()
    } catch {
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }
  }
}
