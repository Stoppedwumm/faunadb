package fauna.core

import java.lang.reflect.{ Field, Method }

class DynAccessor(val obj: AnyRef, path: String) {

  def /(name: String) = {
    if (obj eq null) {
      throw new IllegalArgumentException(s"Can't evaluate [$name] on [$path], it is null.")
    } else {
      eval(name) match {
        case Some(v) => new DynAccessor(v, s"$path/$name")
        case None =>
          throw new IllegalArgumentException(s"Can't evaluate [$name] on [$path], an instance of [${obj.getClass}].")
      }
    }
  }

  def get[T] = obj.asInstanceOf[T]

  private def getField(clazz: Class[_], name: String): Option[Field] =
    getFieldOpt(clazz, name) orElse {
      val qname = clazz.getName.replace('.', '$').concat("$$").concat(name)
      getFieldOpt(clazz, qname) orElse {
        Option(clazz.getSuperclass) flatMap {
          getField(_, name)
        }
      }
    }

  private def getMethod(clazz: Class[_], name: String): Option[Method] =
    getMethodOpt(clazz, name) orElse {
      Option(clazz.getSuperclass) flatMap {
        getMethod(_, name)
      }
    }

  private def getFieldOpt(clazz: Class[_], name: String): Option[Field] =
    try {
      val f = clazz.getDeclaredField(name)
      f.setAccessible(true)
      Some(f)
    } catch {
      case _: NoSuchFieldException => None
    }

  private def getMethodOpt(clazz: Class[_], name: String): Option[Method] =
    try {
      val f = clazz.getDeclaredMethod(name)
      f.setAccessible(true)
      Some(f)
    } catch {
      case _: NoSuchMethodException => None
    }

  private def eval(name: String) = {
    val clazz = obj.getClass
    getField(clazz, name) map { f =>
      f.get(obj)
    } orElse {
      getMethod(clazz, name) map { m =>
        m.invoke(obj)
      }
    }
  }

  override def toString =
    String.valueOf(obj)
}
