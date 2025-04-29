package fauna.repo.values

sealed trait ValueType {

  /** The narrowed runtime representation type of this ValueType */
  type Repr <: Value

  def displayString: String
}

// TODO: This duplicates the structure in repo/schema/ValueType. Need to
// clean this up when we get back to schema.

/** Represents the types of values */
object ValueType {
  final case class AliasType(name: String) extends ValueType {
    def displayString = name
  }

  val ScalarType = AliasType("Scalar")

  // is a ScalarType
  final case class SingletonType(value: Value.Scalar) extends ValueType {
    type Repr = Value.Scalar
    def dynamicType: DynamicType = value.dynamicType
    // FIXME this will get singleton strings incorrect because toString doesn't
    // render with quotes.
    // will want displayString on values as well
    def displayString = value.toString()
  }

  // trait for types which can be derived from a runtime value
  sealed trait DynamicType extends ValueType

  case object AnyType extends ValueType {
    type Repr = Value
    def displayString = "Any"
  }
  case object IDType extends DynamicType {
    type Repr = Value.ID
    def displayString = "ID"
  }
  case object NullType extends DynamicType {
    type Repr = Value.Null
    def displayString = "Null"
  }
  case object BooleanType extends DynamicType {
    type Repr = Value.Boolean
    def displayString = "Boolean"
  }

  // TODO: expand our number hierarchy. Something like:
  // - number
  //   - integer (backed smallest repr possible, or BigInt)
  //     - int16, int32, int64, etc. (backed by short, int, long, etc.)
  //   - decimal (backed by BigDecimal)
  //   - float
  //     - float64, float32, etc.
  case object NumberType extends ValueType {
    type Repr = Value.Number
    def displayString = "Number"
  }
  case object IntType extends DynamicType {
    type Repr = Value.Int
    def displayString = "Int"
  }
  case object LongType extends DynamicType {
    type Repr = Value.Long
    def displayString = "Long"
  }
  case object DoubleType extends DynamicType {
    type Repr = Value.Double
    def displayString = "Double"
  }

  case object StringType extends DynamicType {
    type Repr = Value.Str
    def displayString = "String"
  }
  case object BytesType extends DynamicType {
    type Repr = Value.Bytes
    def displayString = "Bytes"
  }
  case object TimeType extends DynamicType {
    type Repr = Value.Time
    def displayString = "Time"
  }
  // TODO: this type should be able to participate in all places TimeType
  // can, however this is complicated. See note on Value.TransactionTime
  case object TransactionTimeType extends DynamicType {
    type Repr = Value.TransactionTime.type
    def displayString = "TransactionTime"
  }
  case object DateType extends DynamicType {
    type Repr = Value.Date
    def displayString = "Date"
  }
  // FIXME: Get rid of this?
  case object UUIDType extends DynamicType {
    type Repr = Value.UUID
    def displayString = "UUID"
  }

  case object AnyObjectType extends ValueType {
    type Repr = Value.Object
    def displayString = "{ *: Any }"
  }
  case object AnyStructType extends DynamicType {
    type Repr = Value.Struct
    def displayString = "{ *: Any }"
  }
  case object AnyDocType extends DynamicType {
    type Repr = Value.Doc
    def displayString = "Doc"
  }
  case object AnyNullDocType extends DynamicType {
    type Repr = Value.Doc
    def displayString = "NullDoc"
  }
  case class SingletonObjectType(name: String) extends DynamicType {
    type Repr = Value.SingletonObject
    def displayString = name
  }

  case object SetCursorType extends DynamicType {
    type Repr = Value.SetCursor
    def displayString = "SetCursor"
  }

  case object AnyArrayType extends DynamicType {
    type Repr = Value.Array
    def displayString = "Array<Any>"
  }
  case object AnySetType extends DynamicType {
    type Repr = Value.Set
    def displayString = "Set<Any>"
  }

  case object AnyEventSource extends DynamicType {
    type Repr = Value.EventSource
    def displayString = "EventSource<Any>"
  }

  case object AnyFunctionType extends DynamicType {
    type Repr = Value.Func
    def displayString = "Function"
  }
  case object AnyLambdaType extends DynamicType {
    type Repr = Value.Lambda
    def displayString = "Lambda"
  }
}
