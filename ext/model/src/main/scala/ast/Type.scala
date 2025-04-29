package fauna.ast

import fauna.atoms._

sealed abstract class Type(val name: String, val supertype: Type) {
  override def toString = name
}

object Type {
  case object Any extends Type("Any", null)
  case object Scalar extends Type("Scalar", Any)

  case object Number extends Type("Number", Scalar)
  case object Double extends Type("Double", Number)
  case object Integer extends Type("Integer", Number)
  case object Boolean extends Type("Boolean", Scalar)
  case object String extends Type("String", Scalar)
  case object Null extends Type("Null", Scalar)
  case object Action extends Type("Action", Scalar)
  case object Bytes extends Type("Bytes", Scalar)
  case object UUID extends Type("UUID", Scalar)

  case object TransactionTime extends Type("Unresolved Transaction Time", Scalar)

  case object Object extends Type("Object", Any)

  case object Lambda extends Type("Lambda", Any)

  case object AbstractIterable extends Type("Array, Set, or Page", Any)
  case object AbstractArray extends Type("Array or Page", AbstractIterable)
  case object Page extends Type("Page", AbstractArray)
  case object Array extends Type("Array", AbstractArray)

  case object Identifier extends Type("Identifier", Scalar)

  case object Ref extends Type("Ref", Identifier)
  case object CollectionRef extends Type("Collection Ref", Ref)
  case object IndexRef extends Type("Index Ref", Ref)
  case object DatabaseRef extends Type("Database Ref", Ref)
  case object KeyRef extends Type("Key Ref", Ref)
  case object UserFunctionRef extends Type("User Function Ref", Ref)
  case object RoleRef extends Type("Role Ref", Ref)
  case object AccessProviderRef extends Type("Access Provider Ref", Ref)

  case object Set extends Type("Set", Identifier)

  case object Temporal extends Type("Temporal", Scalar)
  case object Time extends Type("Time", Temporal)
  case object Date extends Type("Date", Temporal)
  case object MidstUnit extends Type("Time Unit", Scalar)

  case object Field extends Type("Field/Value", Array)

  @annotation.tailrec
  final def isSub(t: Type, subt: Type): Boolean =
    if (t == subt) true else if (subt eq null) false else isSub(t, subt.supertype)

  final def getType(r: Literal): Type = r match {
    case _: LongL              => Integer
    case _: DoubleL            => Double
    case _: BoolL              => Boolean
    case NullL                 => Null
    case _: StringL            => String
    case _: ActionL            => Action
    case _: BytesL             => Bytes
    case _: UUIDL              => UUID

    case TransactionTimeL       => TransactionTime
    case TransactionTimeMicrosL => TransactionTime

    case CursorL(Left(_))      => Object
    case CursorL(Right(_))     => Array

    // expressing this using ID.unapply opts this expression out of
    // the exhaustiveness checker, which is bad. we nested this match
    // to get exhaustiveness back.
    case ref: RefL =>
      ref.id match {
        case CollectionID(_)   => CollectionRef
        case IndexID(_)        => IndexRef
        case DatabaseID(_)     => DatabaseRef
        case KeyID(_)          => KeyRef
        case UserFunctionID(_) => UserFunctionRef
        case _                 => Ref
      }

    case UnresolvedRefL(r)     => r.rtype

    case _: LambdaL            => Lambda

    case SetL(_)               => Set
    case TimeL(_)              => Time
    case DateL(_)              => Date
    case VersionL(_, _)        => Object
    case DocEventL(_)          => Object
    case SetEventL(_)          => Object
    case ElemL(_, _)           => Object
    case PageL(_, _, _, _)     => Page
    case ArrayL(_)             => Array
    case ObjectL(_)            => Object
  }
}
