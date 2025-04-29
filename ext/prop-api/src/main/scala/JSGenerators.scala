package fauna.prop.api

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.lang.clocks.Clock
import fauna.prop._

trait JSGenerators extends Generators {
  import Prop._

  val jsName = aName map { JSString(_) }
  val jsUniqueName = aUniqueName map { JSString(_) }
  val jsUniqueString = aUniqueString map { JSString(_) }
  val jsUniqueLong = aUniqueLong map { JSLong(_) }
  val jsValue: Prop[JSValue] = jsValue()

  def jsValue(
    minSize: Int = 0,
    maxSize: Int = 10,
    maxDepth: Int = 5,
    allowNull: Boolean = false
  ): Prop[JSValue] = {
    def jsValue0(choice: Int): Prop[JSValue] = choice match {
      case 0 => jsNull
      case 1 => jsLong
      case 2 => jsBoolean
      case 3 => jsDouble
      case 4 => jsString
      case 5 => jsArray(minSize, maxSize, maxDepth, allowNull)
      case 6 => jsObject(minSize, maxSize, maxDepth, allowNull)
    }

    val min = if (allowNull) 0 else 1
    val max = if (maxDepth <= 0) 4 else 6
    int(min to max) flatMap { jsValue0(_) }
  }

  val jsScalarValue = jsValue(maxDepth = 0)

  def jsLong(limit: Long = Long.MaxValue) = long(limit) map { JSLong(_) }

  val jsLong: Prop[JSLong] = jsLong()

  val jsNull = const(JSNull)

  val jsBoolean = boolean map { JSBoolean(_) }

  val jsDouble = double map { JSDouble(_) }

  val jsUUID = uuid map { u =>
    JSString(u.toString)
  }

  val jsURL = aURL map { JSString(_) }

  def jsChars = Prop.char

  def jsString(
    minSize: Int,
    maxSize: Int,
    chars: Iterable[Char]): Prop[JSString] =
    jsString(minSize, maxSize, Prop.choose(chars))

  def jsString(
    minSize: Int,
    maxSize: Int,
    chars: Prop[Char] = jsChars
  ): Prop[JSString] =
    string(minSize, maxSize, chars) map { JSString(_) }


  val jsString: Prop[JSString] = jsString(0, 20)

  val jsArray: Prop[JSArray] = jsArray()

  def jsArray(
    minSize: Int = 0,
    maxSize: Int = 10,
    maxDepth: Int = 10,
    allowNull: Boolean = false
  ) =
    int(minSize to maxSize) flatMap { length =>
      int(maxDepth) flatMap { depth =>
        val values = 0 until length map { _ =>
          jsValue(minSize, maxSize, depth - 1, allowNull)
        }
        (values foldRight const[List[JSValue]](Nil)) { (vp, lp) =>
          lp flatMap { l =>
            vp map { v =>
              v +: l
            }
          }
        } map { vs =>
          JSArray(vs: _*)
        }
      }
    }

  val jsObject: Prop[JSObject] = jsObject()

  def jsObject(
    minSize: Int = 0,
    maxSize: Int = 10,
    maxDepth: Int = 5,
    allowNull: Boolean = false
  ) =
    int(minSize to maxSize) flatMap { length =>
      int(maxDepth) flatMap { depth =>
        val values = 0 until length map { _ =>
          string(10, 40, jsChars) flatMap { key =>
            jsValue(minSize, maxSize, depth - 1, allowNull) map { key -> _ }
          }
        }

        (values foldRight const[List[(String, JSValue)]](Nil)) { (vp, lp) =>
          lp flatMap { l =>
            vp map { v =>
              v +: l
            }
          }
        } map { vs =>
          JSObject(vs: _*)
        }
      }
    }

  def jsObject(
    field: (String, Prop[_ <: JSValue]),
    fields: (String, Prop[_ <: JSValue])*
  ): Prop[JSObject] =
    (field +: fields).foldLeft(const(JSObject.empty)) {
      case (obj, (n, p)) =>
        obj flatMap { o =>
          p map { v =>
            o :+ (n -> v)
          }
        }
    }

  val jsDate = Prop.isoDate map { JSString(_) }
  def jsTimestamp(limit: Timestamp = Clock.time) = Prop.isoTime(limit) map { JSString(_) }

  def jsAlphaNumString(minSize: Int, maxSize: Int) =
    alphaNumString(minSize, maxSize) map { JSString(_) }

  def jsNumericString(minSize: Int, maxSize: Int) =
    numericString(minSize, maxSize) map { JSString(_) }
}
