package fauna.prop.api

import fauna.codex.json._
import language.experimental.macros

/*
 *  There are many unique items with macro which was learned during the
 *  development of multiple versions of query helpers.   The inability
 *  to override a method containing a macro.  The always calling the abstract
 *  definition.  If you decide to improve the flow below, just be aware of
 *  the many pitfalls.
 */

trait DefaultQueryHelpers extends Query20Helpers

trait Query20Helpers extends AbstractQueryHelpers {

  override def ClassesRef = RefV("classes")

  override def RefV(id: JSValue, cls: JSValue): JSObject = RefVHack(id, cls)
  private def RefVHack(id: Any, cls: Any): JSObject = macro QueryHelpersImpl.RefV2

  override def RefV(id: JSValue, cls: JSValue, scope: JSValue): JSObject =
    RefVHack(id, cls, scope)
  private def RefVHack(id: Any, cls: Any, scope: Any): JSObject =
    macro QueryHelpersImpl.RefV3

  override def RefV(id: JSValue, cls: Option[JSValue], scope: Option[JSValue]) = {
    val obj = JSObject.newBuilder
    obj += ("id" -> id)

    cls foreach { cls =>
      obj += ("class" -> cls)
    }

    scope foreach { scope =>
      obj += ("database" -> scope)
    }

    JSObject("@ref" -> obj.result())
  }

  override def ClassRef(name: JSValue): JSObject = ClassRefHack(name)
  private def ClassRefHack(name: JSValue): JSObject = macro QueryHelpersImpl.ClassRef1

  override def ClassRef(name: JSValue, scope: JSValue): JSObject =
    ClassRefHack(name, scope)
  private def ClassRefHack(name: JSValue, scope: JSValue): JSObject =
    macro QueryHelpersImpl.ClassRef2

  override def ClassesNativeClassRef = NativeClassRef("classes", JSNull)
  override def ClassesNativeClassRef(scope: JSValue) =
    NativeClassRef("classes", scope)
}

trait Query27Helpers extends AbstractQueryHelpers {

  override def ClassesRef = RefV("collections")

  override def RefV(id: JSValue, cls: JSValue): JSObject = RefVHack(id, cls)
  private def RefVHack(id: Any, cls: Any): JSObject = macro QueryHelpersImpl27.RefV2

  override def RefV(id: JSValue, cls: JSValue, scope: JSValue): JSObject =
    RefVHack(id, cls, scope)
  private def RefVHack(id: Any, cls: Any, scope: Any): JSObject =
    macro QueryHelpersImpl27.RefV3

  override def RefV(id: JSValue, cls: Option[JSValue], scope: Option[JSValue]) = {
    val obj = JSObject.newBuilder
    obj += ("id" -> id)

    cls foreach { cls =>
      obj += ("collection" -> cls)
    }

    scope foreach { scope =>
      obj += ("database" -> scope)
    }

    JSObject("@ref" -> obj.result())
  }

  override def ClassRef(name: JSValue): JSObject = ClassRefHack(name)
  private def ClassRefHack(name: JSValue): JSObject =
    macro QueryHelpersImpl27.ClassRef1

  override def ClassRef(name: JSValue, scope: JSValue): JSObject =
    ClassRefHack(name, scope)
  private def ClassRefHack(name: JSValue, scope: JSValue): JSObject =
    macro QueryHelpersImpl27.ClassRef2

  override def ClassesNativeClassRef = NativeClassRef("collections", JSNull)
  override def ClassesNativeClassRef(scope: JSValue) =
    NativeClassRef("collections", scope)
}

trait AbstractQueryHelpers {
  // query helpers

  def Ref(str: Any): JSObject = macro QueryHelpersImpl.LegacyRefV

  def RefV(id: Any): JSObject = macro QueryHelpersImpl.RefV1

  def RefV(id: JSValue, cls: JSValue): JSObject

  def RefV(id: JSValue, cls: JSValue, scope: JSValue): JSObject

  def RefV(id: JSValue, cls: Option[JSValue], scope: Option[JSValue]): JSObject

  def DBRefV(name: String, scope: JSObject) =
    RefV(name, Some(DatabasesRef), Some(scope))
  def DBRefV(name: String) = RefV(name, DatabasesRef)

  def ClsRefV(name: String, scope: JSObject) =
    RefV(name, Some(ClassesRef), Some(scope))
  def ClsRefV(name: String) = RefV(name, ClassesRef)

  def IdxRefV(name: String, scope: JSObject) =
    RefV(name, Some(IndexesRef), Some(scope))
  def IdxRefV(name: String) = RefV(name, IndexesRef)

  def FnRefV(name: String, scope: JSObject) =
    RefV(name, Some(FunctionsRef), Some(scope))
  def FnRefV(name: String) = RefV(name, FunctionsRef)

  def RoleRefV(name: String, scope: JSObject) =
    RefV(name, Some(RolesRef), Some(scope))
  def RoleRefV(name: String) = RefV(name, RolesRef)

  def MkRef(cls: Any, id: Any): JSObject = macro QueryHelpersImpl.Ref2
  def MkRef(cls: Any, id: Any, scope: Any): JSObject = macro QueryHelpersImpl.Ref3

  def MkRef(cls: Option[JSValue], id: JSValue, scope: Option[JSValue]) = {
    val obj = JSObject.newBuilder

    cls foreach { cls =>
      obj += ("ref" -> cls)
    }

    scope foreach { scope =>
      obj += ("database" -> scope)
    }

    obj += ("id" -> id)

    obj.result()
  }

  val DatabasesRef = RefV("databases")
  def ClassesRef: JSObject
  val IndexesRef = RefV("indexes")
  val FunctionsRef = RefV("functions")
  val RolesRef = RefV("roles")
  val KeysRef = RefV("keys")
  val TasksRef = RefV("tasks")
  val TokensRef = RefV("tokens")
  val CredentialsRef = RefV("credentials")

  def NativeClassRef(name: String, scope: JSValue): JSObject =
    macro QueryHelpersImpl.NativeClassRef

  val DatabaseNativeClassRef = NativeClassRef("databases", JSNull)
  def DatabaseNativeClassRef(scope: JSValue) = NativeClassRef("databases", scope)

  def ClassesNativeClassRef: JSObject
  def ClassesNativeClassRef(scope: JSValue): JSObject

  val IndexesNativeClassRef = NativeClassRef("indexes", JSNull)
  def IndexesNativeClassRef(scope: JSValue) = NativeClassRef("indexes", scope)

  val KeysNativeClassRef = NativeClassRef("keys", JSNull)
  def KeysNativeClassRef(scope: JSValue) = NativeClassRef("keys", scope)

  val TokensNativeClassRef = NativeClassRef("tokens", JSNull)
  def TokensNativeClassRef(scope: JSValue) = NativeClassRef("tokens", scope)

  val CredentialsNativeClassRef = NativeClassRef("credentials", JSNull)

  def CredentialsNativeClassRef(scope: JSValue) =
    NativeClassRef("credentials", scope)

  val FunctionsNativeClassRef = NativeClassRef("functions", JSNull)
  def FunctionsNativeClassRef(scope: JSValue) = NativeClassRef("functions", scope)

  val RolesNativeClassRef = NativeClassRef("roles", JSNull)
  def RolesNativeClassRef(scope: JSValue) = NativeClassRef("roles", scope)

  val AccessProvidersNativeClassRef = NativeClassRef("access_providers", JSNull)
  def AccessProvidersNativeClassRef(scope: JSValue) = NativeClassRef("access_providers", scope)

  def DatabaseRef(name: String): JSObject = macro QueryHelpersImpl.DatabaseRef1

  def DatabaseRef(name: String, scope: JSValue): JSObject =
    macro QueryHelpersImpl.DatabaseRef2

  def ClassRef(name: JSValue): JSObject

  def ClassRef(name: JSValue, scope: JSValue): JSObject

  def IndexRef(name: String): JSObject = macro QueryHelpersImpl.IndexRef1

  def IndexRef(name: String, scope: JSValue): JSObject =
    macro QueryHelpersImpl.IndexRef2

  def FunctionRef(name: Any): JSObject = macro QueryHelpersImpl.FunctionRef1

  def FunctionRef2(name: Any, scope: JSValue): Any =
    macro QueryHelpersImpl.FunctionRef2

  def RoleRef(name: Any): JSObject = macro QueryHelpersImpl.RoleRef1
  def RoleRef(name: Any, scope: Any): JSObject = macro QueryHelpersImpl.RoleRef2

  def AccessProviderRef(name: Any): JSObject = macro QueryHelpersImpl.AccessProviderRef1
  def AccessProviderRef(name: Any, scope: Any): JSObject = macro QueryHelpersImpl.AccessProviderRef2

  def NextID(): JSObject =
    macro QueryHelpersImpl.NextID // Deprecated in favor of NewID
  def NewID(): JSObject = macro QueryHelpersImpl.NewID

  def SetRef(v: Any): JSObject = macro QueryHelpersImpl.SetRef

  def TS(v: Any): JSObject = macro QueryHelpersImpl.TS

  def Date(v: Any): JSObject = macro QueryHelpersImpl.Date

  def Bytes(v: Any): JSObject = macro QueryHelpersImpl.Bytes

  def UUID(v: Any): JSObject = macro QueryHelpersImpl.UUID

  def At(ts: Any, expr: Any): JSObject = macro QueryHelpersImpl.At

  def Var(name: Any): JSObject = macro QueryHelpersImpl.Var

  def Call(name: Any, args: Any*): JSObject = macro QueryHelpersImpl.Call

  def Let(bindings: Any*)(in: Any): JSObject = macro QueryHelpersImpl.Let

  def If(cond: Any, `then`: Any, `else`: Any): JSObject = macro QueryHelpersImpl.If

  def Abort(v: Any): JSObject = macro QueryHelpersImpl.Abort

  def Quote(v: Any): JSObject = macro QueryHelpersImpl.Quote

  def QueryF(v: Any): JSObject = macro QueryHelpersImpl.QueryF

  def MkObject(fields: Any*): JSObject = macro QueryHelpersImpl.MkObject

  def Do(statements: Any*): JSObject = macro QueryHelpersImpl.Do

  def Lambda(arg: Any): JSObject = macro QueryHelpersImpl.Lambda

  def MapF(lambda: Any, collection: Any): JSObject = macro QueryHelpersImpl.MapF

  def Foreach(lambda: Any, collection: Any): JSObject =
    macro QueryHelpersImpl.Foreach

  def Prepend(elems: Any, coll: Any): JSObject = macro QueryHelpersImpl.Prepend

  def Append(elems: Any, coll: Any): JSObject = macro QueryHelpersImpl.Append

  def Filter(lambda: Any, collection: Any): JSObject = macro QueryHelpersImpl.Filter

  def Take(num: Any, coll: Any): JSObject = macro QueryHelpersImpl.Take

  def Drop(num: Any, coll: Any): JSObject = macro QueryHelpersImpl.Drop

  def IsEmpty(coll: Any): JSObject = macro QueryHelpersImpl.IsEmpty

  def IsNonEmpty(coll: Any): JSObject = macro QueryHelpersImpl.IsNonEmpty

  def Contains(path: Any, in: Any): JSObject = macro QueryHelpersImpl.Contains

  def ContainsPath(path: Any, in: Any): JSObject = macro QueryHelpersImpl.ContainsPath

  def ContainsField(field: Any, in: Any): JSObject = macro QueryHelpersImpl.ContainsField

  def ContainsValue(value: Any, in: Any): JSObject = macro QueryHelpersImpl.ContainsValue

  def Select(path: Any, from: Any): JSObject = macro QueryHelpersImpl.Select2

  def Select(path: Any, from: Any, default: Any): JSObject =
    macro QueryHelpersImpl.Select3

  def Select(path: Any, from: Any, default: Any, all: Any): JSObject =
    macro QueryHelpersImpl.Select4

  def SelectAll(path: Any, from: Any): JSObject = macro QueryHelpersImpl.SelectAll

  def SelectAsIndex(path: Any, from: Any): JSObject =
    macro QueryHelpersImpl.SelectAsIndex

  def Merge(left: Any, right: Any): JSObject = macro QueryHelpersImpl.Merge2

  def Merge(left: Any, right: Any, lambda: Any): JSObject =
    macro QueryHelpersImpl.Merge3

  /*
   * String Functions
   */
  def Concat(v: Any): JSObject = macro QueryHelpersImpl.Concat1

  def Concat(v: Any, separator: Any): JSObject = macro QueryHelpersImpl.Concat2

  def CaseFold(v: Any): JSObject = macro QueryHelpersImpl.CaseFold1

  def CaseFold(v: Any, normalizer: Any): JSObject = macro QueryHelpersImpl.CaseFold2

  def RegexEscape(v: Any): JSObject = macro QueryHelpersImpl.RegexEscape

  def StartsWith(v: Any, search: Any): JSObject = macro QueryHelpersImpl.StartsWith

  def EndsWith(v: Any, search: Any): JSObject = macro QueryHelpersImpl.EndsWith

  def ContainsStr(v: Any, search: Any): JSObject = macro QueryHelpersImpl.ContainsStr

  def ContainsStrRegex(v: Any, pattern: Any): JSObject = macro QueryHelpersImpl.ContainsStrRegex

  def FindStr(v: Any, find: Any): JSObject = macro QueryHelpersImpl.FindStr

  def FindStr(v: Any, find: Any, start: Any): JSObject =
    macro QueryHelpersImpl.FindStr2

  def FindStrRegex(v: Any, pattern: Any): Any =
    macro QueryHelpersImpl.FindStrRegex2

  def FindStrRegex(v: Any, pattern: Any, start: Any): Any =
    macro QueryHelpersImpl.FindStrRegex3

  def FindStrRegex(v: Any, pattern: Any, start: Any, numResults: Any): Any =
    macro QueryHelpersImpl.FindStrRegex4

  def Length(v: Any): JSObject = macro QueryHelpersImpl.Length

  def LowerCase(v: Any): JSObject = macro QueryHelpersImpl.LowerCase

  def LTrim(v: Any): JSObject = macro QueryHelpersImpl.LTrim

  def NGram(v: Any): JSObject = macro QueryHelpersImpl.NGram1

  def NGram(v: Any, min: Any): JSObject = macro QueryHelpersImpl.NGram2

  def NGram(v: Any, min: Any, max: Any): JSObject = macro QueryHelpersImpl.NGram3

  def RepeatString(v: Any): JSObject = macro QueryHelpersImpl.RepeatString

  def RepeatString(v: Any, number: Any): JSObject =
    macro QueryHelpersImpl.RepeatString2

  def ReplaceStr(v: Any, find: Any, replace: Any): Any =
    macro QueryHelpersImpl.ReplaceString3

  def ReplaceStrRegex(v: Any, pattern: Any, replace: Any): Any =
    macro QueryHelpersImpl.ReplaceStrRegex3

  def ReplaceStrRegex(v: Any, pattern: Any, replace: Any, first: Any): Any =
    macro QueryHelpersImpl.ReplaceStrRegex4

  def RTrim(v: Any): JSObject = macro QueryHelpersImpl.RTrim

  def Space(length: Any): JSObject = macro QueryHelpersImpl.Space

  def SubString(v: Any): JSObject = macro QueryHelpersImpl.SubString

  def SubString(v: Any, start: Any): JSObject = macro QueryHelpersImpl.SubString2

  def SubString3(v: Any, start: Any, length: Any): Any =
    macro QueryHelpersImpl.SubString3

  def TitleCase(v: Any): JSObject = macro QueryHelpersImpl.TitleCase

  def Trim(v: Any): JSObject = macro QueryHelpersImpl.Trim

  def UpperCase(v: Any): JSObject = macro QueryHelpersImpl.UpperCase

  def Format(fmt: Any, values: Any): JSObject = macro QueryHelpersImpl.Format

  def SplitStr(v: Any, token: Any): JSObject = macro QueryHelpersImpl.SplitStr

  def SplitStr(v: Any, token: Any, count: Any): JSObject =
    macro QueryHelpersImpl.SplitStr2

  def SplitStrRegex(v: Any, pattern: Any): JSObject =
    macro QueryHelpersImpl.SplitStrRegex

  def SplitStrRegex(v: Any, pattern: Any, count: Any): JSObject =
    macro QueryHelpersImpl.SplitStrRegex2

  // Numeric Functions

  def Abs(v: Any): JSObject = macro QueryHelpersImpl.Abs

  def ACos(v: Any): JSObject = macro QueryHelpersImpl.ACos

  def AddF(vs: Any*): JSObject = macro QueryHelpersImpl.AddF

  def ASin(v: Any): JSObject = macro QueryHelpersImpl.ASin

  def ATan(v: Any): JSObject = macro QueryHelpersImpl.ATan

  def BitAnd(vs: Any*): JSObject = macro QueryHelpersImpl.BitAnd

  def BitNot(v: Any): JSObject = macro QueryHelpersImpl.BitNot

  def BitOr(vs: Any*): JSObject = macro QueryHelpersImpl.BitOr

  def BitXor(vs: Any*): JSObject = macro QueryHelpersImpl.BitXor

  def Ceil(v: Any): JSObject = macro QueryHelpersImpl.Ceil

  def Cos(v: Any): JSObject = macro QueryHelpersImpl.Cos

  def Cosh(v: Any): JSObject = macro QueryHelpersImpl.Cosh

  def Degrees(v: Any): JSObject = macro QueryHelpersImpl.Degrees

  def Divide(vs: Any*): JSObject = macro QueryHelpersImpl.Divide

  def Exp(v: Any): JSObject = macro QueryHelpersImpl.Exp

  def Hypot(v: Any): JSObject = macro QueryHelpersImpl.Hypot

  def Hypot(v: Any, v2: Any): JSObject = macro QueryHelpersImpl.Hypot2

  def Floor(v: Any): JSObject = macro QueryHelpersImpl.Floor

  def Ln(v: Any): JSObject = macro QueryHelpersImpl.Ln

  def Log(v: Any): JSObject = macro QueryHelpersImpl.Log

  def Max(vs: Any*): JSObject = macro QueryHelpersImpl.Max

  def Min(vs: Any*): JSObject = macro QueryHelpersImpl.Min

  def Modulo(vs: Any*): JSObject = macro QueryHelpersImpl.Modulo

  def Multiply(vs: Any*): JSObject = macro QueryHelpersImpl.Multiply

  def Radians(v: Any): JSObject = macro QueryHelpersImpl.Radians

  def Round(v: Any): JSObject = macro QueryHelpersImpl.Round

  def Round(v: Any, dp: Any): JSObject = macro QueryHelpersImpl.Round2

  def Pow(v: Any): JSObject = macro QueryHelpersImpl.Pow

  def Pow(v: Any, e: Any): JSObject = macro QueryHelpersImpl.Pow2

  def Sign(v: Any): JSObject = macro QueryHelpersImpl.Sign

  def Sin(v: Any): JSObject = macro QueryHelpersImpl.Sin

  def Sinh(v: Any): JSObject = macro QueryHelpersImpl.Sinh

  def Sqrt(v: Any): JSObject = macro QueryHelpersImpl.Sqrt

  def Subtract(vs: Any*): JSObject = macro QueryHelpersImpl.Subtract

  def Tan(v: Any): JSObject = macro QueryHelpersImpl.Tan

  def Tanh(v: Any): JSObject = macro QueryHelpersImpl.Tanh

  def Trunc(v: Any): JSObject = macro QueryHelpersImpl.Trunc

  def Trunc(v: Any, dp: Any): JSObject = macro QueryHelpersImpl.Trunc2

  def Sum(coll: Any): JSObject = macro QueryHelpersImpl.Sum

  def Count(coll: Any): JSObject = macro QueryHelpersImpl.Count

  def Mean(coll: Any): JSObject = macro QueryHelpersImpl.Mean

  def Equals(vs: Any*): JSObject = macro QueryHelpersImpl.Equals

  def And(vs: Any*): JSObject = macro QueryHelpersImpl.And

  def Or(vs: Any*): JSObject = macro QueryHelpersImpl.Or

  def Not(v: Any): JSObject = macro QueryHelpersImpl.Not

  def Any(coll: Any): JSObject = macro QueryHelpersImpl.Any

  def All(coll: Any): JSObject = macro QueryHelpersImpl.All

  def LessThan(vs: Any*): JSObject = macro QueryHelpersImpl.LessThan

  def LessThanOrEquals(vs: Any*): JSObject = macro QueryHelpersImpl.LessThanOrEquals

  def GreaterThan(vs: Any*): JSObject = macro QueryHelpersImpl.GreaterThan

  def GreaterThanOrEquals(vs: Any*): JSObject =
    macro QueryHelpersImpl.GreaterThanOrEquals

  def Singleton(ref: Any): JSObject = macro QueryHelpersImpl.Singleton

  def Events(set: Any): JSObject = macro QueryHelpersImpl.Events

  def Documents(set: Any): JSObject = macro QueryHelpersImpl.Documents

  def Match(index: Any): JSObject = macro QueryHelpersImpl.Match1

  def Match(index: Any, terms: Any): JSObject = macro QueryHelpersImpl.Match2

  def Union(sets: Any*): JSObject = macro QueryHelpersImpl.Union

  def Intersection(sets: Any*): JSObject = macro QueryHelpersImpl.Intersection

  def Difference(sets: Any*): JSObject = macro QueryHelpersImpl.Difference

  def Distinct(set: Any): JSObject = macro QueryHelpersImpl.Distinct

  def RangeF(set: Any, from: Any, to: Any): JSObject = macro QueryHelpersImpl.RangeF

  def Join(source: Any, `with`: Any): JSObject = macro QueryHelpersImpl.Join

  def Reverse(source: Any): JSObject = macro QueryHelpersImpl.Reverse

  def Exists(v: Any): JSObject = macro QueryHelpersImpl.Exists1

  def Exists(v: Any, ts: Any): JSObject = macro QueryHelpersImpl.Exists2

  def Get(v: Any): JSObject = macro QueryHelpersImpl.Get1

  def Get(v: Any, ts: Any): JSObject = macro QueryHelpersImpl.Get2

  def First(set: Any): JSObject = macro QueryHelpersImpl.First

  def Last(set: Any): JSObject = macro QueryHelpersImpl.Last

  def KeyFromSecret(v: Any): JSObject = macro QueryHelpersImpl.KeyFromSecret

  def Reduce(acc: Any, initial: Any, collection: Any): JSObject =
    macro QueryHelpersImpl.Reduce

  sealed trait PaginateCursor
  case class Before(before: JSValue) extends PaginateCursor
  case class After(after: JSValue) extends PaginateCursor
  case class Cursor(value: JSValue) extends PaginateCursor
  case object NoCursor extends PaginateCursor

  object Cursor {
    def apply(c: Before): Cursor = new Cursor(MkObject("before" -> c.before))
    def apply(c: After): Cursor = new Cursor(MkObject("after" -> c.after))

    @annotation.nowarn("cat=unused-params")
    def apply(c: NoCursor.type): Cursor = new Cursor(JSNull)
  }

  def Paginate(
    set: JSValue,
    cursor: PaginateCursor = NoCursor,
    ts: JSValue = JSNull,
    size: JSValue = JSNull,
    events: JSValue = JSNull,
    sources: JSValue = JSNull
  ) = {

    val obj = JSObject.newBuilder

    obj += ("paginate" -> set)

    cursor match {
      case Before(c) => obj += ("before" -> c)
      case After(c)  => obj += ("after" -> c)
      case Cursor(c) => obj += ("cursor" -> c)
      case NoCursor  => ()
    }

    if (ts != JSNull) obj += ("ts" -> ts)
    if (size != JSNull) obj += ("size" -> size)
    if (events != JSNull) obj += ("events" -> events)
    if (sources != JSNull) obj += ("sources" -> sources)

    obj.result()
  }

  def CreateF(cls: Any, params: Any, creds: Any): Any =
    macro QueryHelpersImpl.Create3

  def CreateF(cls: Any, params: Any): JSObject = macro QueryHelpersImpl.Create2

  def CreateF(cls: Any): JSObject = macro QueryHelpersImpl.Create1

  def CreateClass(params: Any): JSObject = macro QueryHelpersImpl.CreateClass

  def CreateCollection(params: Any): JSObject =
    macro QueryHelpersImpl.CreateCollection

  def CreateFunction(params: Any): JSObject = macro QueryHelpersImpl.CreateFunction

  def CreateIndex(params: Any): JSObject = macro QueryHelpersImpl.CreateIndex

  def CreateDatabase(params: Any): JSObject = macro QueryHelpersImpl.CreateDatabase

  def CreateKey(params: Any): JSObject = macro QueryHelpersImpl.CreateKey

  def CreateRole(params: Any): JSObject = macro QueryHelpersImpl.CreateRole

  def CreateAccessProvider(params: Any): JSObject =
    macro QueryHelpersImpl.CreateAccessProvider

  def Update(v: Any, params: Any): JSObject = macro QueryHelpersImpl.Update2

  def Update(v: Any): JSObject = macro QueryHelpersImpl.Update1

  def Replace(v: Any, params: Any): JSObject = macro QueryHelpersImpl.Replace2

  def Replace(v: Any): JSObject = macro QueryHelpersImpl.Replace1

  def DeleteF(v: Any): JSObject = macro QueryHelpersImpl.Delete

  def InsertVers(v: Any, ts: Any, action: Any, params: Any): Any =
    macro QueryHelpersImpl.InsertVers4

  def InsertVers(v: Any, ts: Any, action: Any): Any =
    macro QueryHelpersImpl.InsertVers3

  def RemoveVers(v: Any, ts: Any, action: Any): Any =
    macro QueryHelpersImpl.RemoveVers

  def MoveDatabase(src: Any, dst: Any): JSObject =
    macro QueryHelpersImpl.MoveDatabase

  def HasIdentity(): JSObject = macro QueryHelpersImpl.HasIdentity

  def Identity(): JSObject = macro QueryHelpersImpl.Identity

  def CurrentIdentity(): JSObject = macro QueryHelpersImpl.CurrentIdentity

  def HasCurrentIdentity(): JSObject = macro QueryHelpersImpl.HasCurrentIdentity

  def Login(inst: Any, params: Any): JSObject = macro QueryHelpersImpl.Login

  def Identify(inst: Any, password: Any): JSObject = macro QueryHelpersImpl.Identify

  def Logout(removeAll: Any): JSObject = macro QueryHelpersImpl.Logout

  def IssueAccessJWT(ref: Any): JSObject = macro QueryHelpersImpl.IssueAccessJWT1

  def IssueAccessJWT(ref: Any, expireIn: Any): JSObject =
    macro QueryHelpersImpl.IssueAccessJWT2

  def CurrentToken(): JSObject = macro QueryHelpersImpl.CurrentToken

  def HasCurrentToken(): JSObject = macro QueryHelpersImpl.HasCurrentToken

  def Time(v: Any): JSObject = macro QueryHelpersImpl.Time

  def Now(): JSObject = macro QueryHelpersImpl.Now

  def Epoch(v: Any, unit: Any): JSObject = macro QueryHelpersImpl.Epoch

  def ToMicros(v: Any): JSObject = macro QueryHelpersImpl.ToMicros

  def ToMillis(v: Any): JSObject = macro QueryHelpersImpl.ToMillis

  def ToSeconds(v: Any): JSObject = macro QueryHelpersImpl.ToSeconds

  def Second(v: Any): JSObject = macro QueryHelpersImpl.Second

  def Minute(v: Any): JSObject = macro QueryHelpersImpl.Minute

  def Hour(v: Any): JSObject = macro QueryHelpersImpl.Hour

  def DayOfMonth(v: Any): JSObject = macro QueryHelpersImpl.DayOfMonth

  def DayOfWeek(v: Any): JSObject = macro QueryHelpersImpl.DayOfWeek

  def DayOfYear(v: Any): JSObject = macro QueryHelpersImpl.DayOfYear

  def Month(v: Any): JSObject = macro QueryHelpersImpl.Month

  def Year(v: Any): JSObject = macro QueryHelpersImpl.Year

  def DateF(v: Any): JSObject = macro QueryHelpersImpl.DateF

  def TimeAdd(base: Any, offset: Any, unit: Any): JSObject =
    macro QueryHelpersImpl.TimeAdd

  def TimeSubtract(base: Any, offset: Any, unit: Any): JSObject =
    macro QueryHelpersImpl.TimeSubtract

  def TimeDiff(start: Any, finish: Any, unit: Any): JSObject =
    macro QueryHelpersImpl.TimeDiff

  def ToString(v: Any): JSObject = macro QueryHelpersImpl.ToString

  def ToNumber(v: Any): JSObject = macro QueryHelpersImpl.ToNumber

  def ToDouble(v: Any): JSObject = macro QueryHelpersImpl.ToDouble

  def ToInteger(v: Any): JSObject = macro QueryHelpersImpl.ToInteger

  def ToTime(v: Any): JSObject = macro QueryHelpersImpl.ToTime

  def ToDate(v: Any): JSObject = macro QueryHelpersImpl.ToDate

  def ToObject(v: Any): JSObject = macro QueryHelpersImpl.ToObject

  def ToArray(v: Any): JSObject = macro QueryHelpersImpl.ToArray

  def IsNumber(v: Any): JSObject = macro QueryHelpersImpl.IsNumber

  def IsDouble(v: Any): JSObject = macro QueryHelpersImpl.IsDouble

  def IsInteger(v: Any): JSObject = macro QueryHelpersImpl.IsInteger

  def IsBoolean(v: Any): JSObject = macro QueryHelpersImpl.IsBoolean

  def IsNull(v: Any): JSObject = macro QueryHelpersImpl.IsNull

  def IsBytes(v: Any): JSObject = macro QueryHelpersImpl.IsBytes

  def IsTimestamp(v: Any): JSObject = macro QueryHelpersImpl.IsTimestamp

  def IsDate(v: Any): JSObject = macro QueryHelpersImpl.IsDate

  def IsUUID(v: Any): JSObject = macro QueryHelpersImpl.IsUUID

  def IsString(v: Any): JSObject = macro QueryHelpersImpl.IsString

  def IsArray(v: Any): JSObject = macro QueryHelpersImpl.IsArray

  def IsObject(v: Any): JSObject = macro QueryHelpersImpl.IsObject

  def IsRef(v: Any): JSObject = macro QueryHelpersImpl.IsRef

  def IsSet(v: Any): JSObject = macro QueryHelpersImpl.IsSet

  def IsDoc(v: Any): JSObject = macro QueryHelpersImpl.IsDoc

  def IsLambda(v: Any): JSObject = macro QueryHelpersImpl.IsLambda

  def IsCollection(v: Any): JSObject = macro QueryHelpersImpl.IsCollection

  def IsDatabase(v: Any): JSObject = macro QueryHelpersImpl.IsDatabase

  def IsIndex(v: Any): JSObject = macro QueryHelpersImpl.IsIndex

  def IsFunction(v: Any): JSObject = macro QueryHelpersImpl.IsFunction

  def IsKey(v: Any): JSObject = macro QueryHelpersImpl.IsKey

  def IsToken(v: Any): JSObject = macro QueryHelpersImpl.IsToken

  def IsCredentials(v: Any): JSObject = macro QueryHelpersImpl.IsCredentials

  def IsRole(v: Any): JSObject = macro QueryHelpersImpl.IsRole
}

object DefaultQueryHelpers extends DefaultQueryHelpers
object Query27Helpers extends Query27Helpers
