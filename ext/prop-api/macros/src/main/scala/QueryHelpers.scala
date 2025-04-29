package fauna.prop.api

import scala.reflect.macros._

// FIXME: These are macros in order to get around a terrible slowdown
// in compilation related to a bad interaction between these helpers
// and implicit conversions to JSValue. (For example this cuts doc
// build time from 800 sec to 75 sec.) Replace with regular functions
// when possible.

class QueryHelpersImpl(val c: whitebox.Context) {
  import c.universe._

  def LegacyRefV(str: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" -> $str)"""

  def RefV1(id: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" ->
      _root_.fauna.codex.json.JSObject("id" -> $id))"""

  def RefV2(id: c.Tree, cls: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" ->
      _root_.fauna.codex.json.JSObject("id" -> $id, "class" -> $cls))"""

  def RefV3(id: c.Tree, cls: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" ->
      _root_.fauna.codex.json.JSObject("id" -> $id, "class" -> $cls, "database" -> $scope))"""

  def Ref2(cls: c.Tree, id: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ref" -> $cls, "id" -> $id)"""

  def Ref3(cls: c.Tree, id: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ref" -> $cls, "database" -> $scope, "id" -> $id)"""

  def DatabaseRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("database" -> $name)"""

  def DatabaseRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("database" -> $name, "scope" -> $scope)"""

  def IndexRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("index" -> $name)"""

  def IndexRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("index" -> $name, "scope" -> $scope)"""

  def ClassRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("class" -> $name)"""

  def ClassRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("class" -> $name, "scope" -> $scope)"""

  def FunctionRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("function" -> $name)"""

  def FunctionRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("function" -> $name, "scope" -> $scope)"""

  def RoleRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("role" -> $name)"""

  def RoleRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("role" -> $name, "scope" -> $scope)"""

  def AccessProviderRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("access_provider" -> $name)"""

  def AccessProviderRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("access_provider" -> $name, "scope" -> $scope)"""

  def NativeClassRef(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject($name -> $scope)"""

  def NextID() =
    q"""_root_.fauna.codex.json.JSObject("next_id" -> _root_.fauna.codex.json.JSNull)""" // Deprecated in favor of NewID
  def NewID() = q"""_root_.fauna.codex.json.JSObject("new_id" -> _root_.fauna.codex.json.JSNull)"""

  def SetRef(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("@set" -> $v)"""

  def TS(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("@ts" -> $v)"""

  def Date(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("@date" -> $v)"""

  def Bytes(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@bytes" ->
      _root_.fauna.util.Base64.encodeUrlSafe($v))"""

  def UUID(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@uuid" -> $v)"""

  def Var(name: c.Tree) = q"""_root_.fauna.codex.json.JSObject("var" -> $name)"""

  def Call(name: c.Tree, args: c.Tree*) =
    args match {
      case Seq() =>
        q"""_root_.fauna.codex.json.JSObject("call" -> $name)"""
      case Seq(arg) =>
        q"""_root_.fauna.codex.json.JSObject("call" -> $name, "arguments" -> $arg)"""
      case args =>
        q"""_root_.fauna.codex.json.JSObject(
          "call" -> $name,
          "arguments" -> _root_.fauna.codex.json.JSArray(..$args))"""
    }

  def At(ts: c.Tree, expr: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("at" -> $ts, "expr" -> $expr)"""

  def Let(bindings: c.Tree*)(in: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject(
      "let" -> _root_.fauna.codex.json.JSObject(..$bindings),
      "in" -> $in)"""

  def If(cond: c.Tree, `then`: c.Tree, `else`: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject(
      "if" -> $cond,
      "then" -> ${`then`},
      "else" -> ${`else`})"""

  def Abort(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("abort" -> $v)"""

  def Quote(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("quote" -> $v)"""

  def QueryF(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("query" -> $v)"""

  def MkObject(fields: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "object" -> _root_.fauna.codex.json.JSObject(..$fields))"""

  def Do(statements: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "do" -> _root_.fauna.codex.json.JSArray(..$statements))"""

  def Lambda(arg: c.Tree) = {
    val (pat, expr) = (arg: @unchecked) match {
      case q"($_[$_]($pat)).->[$_]($expr)" => (pat, expr)
      case q"($pat, $expr)"                => (pat, expr)
    }

    q"""_root_.fauna.codex.json.JSObject("lambda" -> $pat, "expr" -> $expr)"""
  }

  def MapF(lambda: c.Tree, collection: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("map" -> $lambda, "collection" -> $collection)"""

  def Foreach(lambda: c.Tree, collection: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("foreach" -> $lambda, "collection" -> $collection)"""

  def Prepend(elems: c.Tree, coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("prepend" -> $elems, "collection" -> $coll)"""

  def Append(elems: c.Tree, coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("append" -> $elems, "collection" -> $coll)"""

  def Filter(lambda: c.Tree, collection: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("filter" -> $lambda, "collection" -> $collection)"""

  def Take(num: c.Tree, coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("take" -> $num, "collection" -> $coll)"""

  def Drop(num: c.Tree, coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("drop" -> $num, "collection" -> $coll)"""

  def IsEmpty(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_empty" -> $coll)"""

  def IsNonEmpty(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_nonempty" -> $coll)"""

  def Contains(path: c.Tree, in: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("contains" -> $path, "in" -> $in)"""

  def ContainsPath(path: c.Tree, in: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("contains_path" -> $path, "in" -> $in)"""

  def ContainsField(field: c.Tree, in: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("contains_field" -> $field, "in" -> $in)"""

  def ContainsValue(value: c.Tree, in: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("contains_value" -> $value, "in" -> $in)"""

  def Select2(path: c.Tree, from: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("select" -> $path, "from" -> $from)"""

  def Select3(path: c.Tree, from: c.Tree, default: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("select" -> $path, "from" -> $from, "default" -> $default)"""

  def Select4(path: c.Tree, from: c.Tree, default: c.Tree, all: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("select" -> $path, "from" -> $from, "default" -> $default, "all" -> $all)"""

  def SelectAll(path: c.Tree, from: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("select_all" -> $path, "from" -> $from)"""

  def SelectAsIndex(path: c.Tree, from: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("select_as_index" -> $path, "from" -> $from)"""

  def Merge2(left: c.Tree, right: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("merge" -> $left, "with" -> $right)"""

  def Merge3(left: c.Tree, right: c.Tree, lambda: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("merge" -> $left, "with" -> $right, "lambda" -> $lambda)"""

  // String Functions

  def Concat1(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("concat" -> $v)"""

  def Concat2(v: c.Tree, separator: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("concat" -> $v, "separator" -> $separator)"""

  def CaseFold1(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("casefold" -> $v)"""

  def CaseFold2(v: c.Tree, normalizer: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("casefold" -> $v, "normalizer" -> $normalizer)"""

  def RegexEscape(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("regexescape" -> $v)"""

  def StartsWith(v: c.Tree, search: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("startswith" -> $v, "search" -> $search)"""

  def EndsWith(v: c.Tree, search: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("endswith" -> $v, "search" -> $search)"""

  def ContainsStr(v: c.Tree, search: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("containsstr" -> $v, "search" -> $search)"""

  def ContainsStrRegex(v: c.Tree, pattern: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("containsstrregex" -> $v, "pattern" -> $pattern)"""

  def FindStr(v: c.Tree, find: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("findstr" -> $v, "find" -> $find)"""

  def FindStr2(v: c.Tree, find: c.Tree, start: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("findstr" -> $v, "find" -> $find, "start" -> $start)"""

  def FindStrRegex2(v: c.Tree, pattern: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("findstrregex" -> $v, "pattern" -> $pattern)"""

  def FindStrRegex3(v: c.Tree, pattern: c.Tree, start: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("findstrregex" -> $v, "pattern" -> $pattern, "start" -> $start)"""

  def FindStrRegex4(v: c.Tree, pattern: c.Tree, start: c.Tree, numResults: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("findstrregex" -> $v, "pattern" -> $pattern, "start" -> $start, "numResults" -> $numResults)"""

  def Length(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("length" -> $v)"""

  def LowerCase(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("lowercase" -> $v)"""

  def LTrim(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("ltrim" -> $v)"""

  def NGram1(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("ngram" -> $v)"""

  def NGram2(v: c.Tree, min: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ngram" -> $v, "min" -> $min)"""

  def NGram3(v: c.Tree, min: c.Tree, max: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ngram" -> $v, "min" -> $min, "max" -> $max)"""

  def RepeatString(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("repeat" -> $v)"""

  def RepeatString2(v: c.Tree, number: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("repeat" -> $v, "number" -> $number)"""

  def ReplaceString3(v: c.Tree, find: c.Tree, replace: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("replacestr" -> $v, "find" -> $find, "replace" -> $replace)"""

  def ReplaceStrRegex3(v: c.Tree, pattern: c.Tree, replace: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("replacestrregex" -> $v, "pattern" -> $pattern, "replace" -> $replace)"""

  def ReplaceStrRegex4(v: c.Tree, pattern: c.Tree, replace: c.Tree, first: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("replacestrregex" -> $v, "pattern" -> $pattern, "replace" -> $replace, "first" -> $first)"""

  def RTrim(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("rtrim" -> $v)"""

  def Space(length: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("space" -> $length)"""

  def SubString(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("substring" -> $v)"""

  def SubString2(v: c.Tree, start: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("substring" -> $v, "start" -> $start)"""

  def SubString3(v: c.Tree, start: c.Tree, length: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("substring" -> $v, "start" -> $start, "length" -> $length)"""

  def TitleCase(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("titlecase" -> $v)"""

  def Trim(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("trim" -> $v)"""

  def UpperCase(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("uppercase" -> $v)"""

  def Format(fmt: c.Tree, values: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("format" -> $fmt, "values" -> $values)"""

  def SplitStr(v: c.Tree, token: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("split_str" -> $v, "token" -> $token)"""

  def SplitStr2(v: c.Tree, token: c.Tree, count: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("split_str" -> $v, "token" -> $token, "count" -> $count)"""

  def SplitStrRegex(v: c.Tree, pattern: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("split_str_regex" -> $v, "pattern" -> $pattern)"""

  def SplitStrRegex2(v: c.Tree, pattern: c.Tree, count: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("split_str_regex" -> $v, "pattern" -> $pattern, "count" -> $count)"""

  // Numeric Functions

  def Abs(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("abs" -> $v)"""

  def ACos(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("acos" -> $v)"""

  def AddF(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "add" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def ASin(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("asin" -> $v)"""

  def ATan(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("atan" -> $v)"""

  def BitAnd(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "bitand" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def BitNot(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("bitnot" -> $v)"""

  def BitOr(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "bitor" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def BitXor(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "bitxor" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Ceil(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ceil" -> $v)"""

  def Cos(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("cos" -> $v)"""

  def Cosh(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("cosh" -> $v)"""

  def Degrees(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("degrees" -> $v)"""

  def Divide(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "divide" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Exp(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("exp" -> $v)"""

  def Floor(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("floor" -> $v)"""

  def Hypot(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("hypot" -> $v)"""

  def Hypot2(v: c.Tree, v2: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("hypot" -> $v, "b" -> $v2)"""

  def Ln(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("ln" -> $v)"""

  def Log(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("log" -> $v)"""

  def Max(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "max" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Min(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "min" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Modulo(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "modulo" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Multiply(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "multiply" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Pow(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("pow" -> $v)"""

  def Pow2(v: c.Tree, e: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("pow" -> $v, "exp" -> $e)"""

  def Radians(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("radians" -> $v)"""

  def Round(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("round" -> $v)"""

  def Round2(v: c.Tree, dp: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("round" -> $v, "precision" -> $dp)"""

  def Sign(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("sign" -> $v)"""

  def Sin(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("sin" -> $v)"""

  def Sinh(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("sinh" -> $v)"""

  def Sqrt(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("sqrt" -> $v)"""

  def Subtract(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "subtract" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Tan(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("tan" -> $v)"""

  def Tanh(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("tanh" -> $v)"""

  def Trunc(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("trunc" -> $v)"""

  def Trunc2(v: c.Tree, dp: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("trunc" -> $v, "precision" -> $dp)"""

  def Sum(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("sum" -> $coll)"""

  def Count(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("count" -> $coll)"""

  def Mean(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("mean" -> $coll)"""

  // Numeric Functions   End

  def Equals(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "equals" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def And(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "and" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Or(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "or" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Not(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("not" -> $v)"""

  def Any(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("any" -> $coll)"""

  def All(coll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("all" -> $coll)"""

  def LessThan(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "lt" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def LessThanOrEquals(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "lte" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def GreaterThan(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "gt" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def GreaterThanOrEquals(vs: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "gte" -> _root_.fauna.codex.json.JSArray(..$vs))"""

  def Singleton(ref: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("singleton" -> $ref)"""

  def Events(set: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("events" -> $set)"""

  def Documents(set: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("documents" -> $set)"""

  def Match1(index: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("match" -> $index)"""

  def Match2(index: c.Tree, terms: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("match" -> $index, "terms" -> $terms)"""

  def Union(sets: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "union" -> _root_.fauna.codex.json.JSArray(..$sets))"""

  def Intersection(sets: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "intersection" -> _root_.fauna.codex.json.JSArray(..$sets))"""

  def Difference(sets: c.Tree*) =
    q"""_root_.fauna.codex.json.JSObject(
      "difference" -> _root_.fauna.codex.json.JSArray(..$sets))"""

  def Distinct(set: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("distinct" -> $set)"""

  def RangeF(set: c.Tree, from: c.Tree, to: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("range" -> $set, "from" -> $from, "to" -> $to)"""

  def Join(source: c.Tree, `with`: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("join" -> $source, "with" -> ${`with`})"""

  def Reverse(source: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("reverse" -> $source)"""

  def Exists1(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("exists" -> $v)"""

  def Exists2(v: c.Tree, ts: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("exists" -> $v, "ts" -> $ts)"""

  def Get1(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("get" -> $v)"""

  def Get2(v: c.Tree, ts: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("get" -> $v, "ts" -> $ts)"""

  def First(set: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("first" -> $set)"""

  def Last(set: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("last" -> $set)"""

  def KeyFromSecret(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("key_from_secret" -> $v)"""

  def Reduce(acc: c.Tree, initial: c.Tree, collection: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject(
      "reduce" -> $acc,
      "initial" -> $initial,
      "collection" -> $collection)"""

  def Create3(cls: c.Tree, params: c.Tree, creds: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject(
      "create" -> $cls,
      "params" -> $params,
      "credentials" -> $creds)"""

  def Create2(cls: c.Tree, params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create" -> $cls, "params" -> $params)"""

  def Create1(cls: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create" -> $cls)"""

  def CreateClass(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_class" -> $params)"""

  def CreateCollection(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_collection" -> $params)"""

  def CreateFunction(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_function" -> $params)"""

  def CreateIndex(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_index" -> $params)"""

  def CreateDatabase(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_database" -> $params)"""

  def CreateKey(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_key" -> $params)"""

  def CreateRole(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_role" -> $params)"""

  def CreateAccessProvider(params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("create_access_provider" -> $params)"""

  def Update2(v: c.Tree, params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("update" -> $v, "params" -> $params)"""

  def Update1(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("update" -> $v)"""

  def Replace2(v: c.Tree, params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("replace" -> $v, "params" -> $params)"""

  def Replace1(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("replace" -> $v)"""

  def Delete(v: c.Tree) = q"""_root_.fauna.codex.json.JSObject("delete" -> $v)"""

  def InsertVers4(v: c.Tree, ts: c.Tree, action: c.Tree, params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject(
      "insert" -> $v,
      "ts" -> $ts,
      "action" -> $action,
      "params" -> $params)"""

  def InsertVers3(v: c.Tree, ts: c.Tree, action: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("insert" -> $v, "ts" -> $ts, "action" -> $action)"""

  def RemoveVers(v: c.Tree, ts: c.Tree, action: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("remove" -> $v, "ts" -> $ts, "action" -> $action)"""

  def MoveDatabase(src: c.Tree, dst: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("move_database" -> $src, "to" -> $dst)"""

  def HasIdentity() =
    q"""_root_.fauna.codex.json.JSObject("has_identity" -> _root_.fauna.codex.json.JSNull)"""

  def Identity() =
    q"""_root_.fauna.codex.json.JSObject("identity" -> _root_.fauna.codex.json.JSNull)"""

  def CurrentIdentity() =
    q"""_root_.fauna.codex.json.JSObject("current_identity" -> _root_.fauna.codex.json.JSNull)"""

  def HasCurrentIdentity() =
    q"""_root_.fauna.codex.json.JSObject("has_current_identity" -> _root_.fauna.codex.json.JSNull)"""

  def Login(inst: c.Tree, params: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("login" -> $inst, "params" -> $params)"""

  def Identify(inst: c.Tree, password: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("identify" -> $inst, "password" -> $password)"""

  def Logout(removeAll: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("logout" -> $removeAll)"""

  def IssueAccessJWT1(ref: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("issue_access_jwt" -> $ref)"""

  def IssueAccessJWT2(ref: c.Tree, expireIn: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("issue_access_jwt" -> $ref, "expire_in" -> $expireIn)"""

  def CurrentToken() =
    q"""_root_.fauna.codex.json.JSObject("current_token" -> _root_.fauna.codex.json.JSNull)"""

  def HasCurrentToken() =
    q"""_root_.fauna.codex.json.JSObject("has_current_token" -> _root_.fauna.codex.json.JSNull)"""

  def Time(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("time" -> $v)"""

  def Now() =
    q"""_root_.fauna.codex.json.JSObject("now" -> _root_.fauna.codex.json.JSNull)"""

  def Epoch(v: c.Tree, unit: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("epoch" -> $v, "unit" -> $unit)"""

  def ToMicros(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_micros" -> $v)"""

  def ToMillis(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_millis" -> $v)"""

  def ToSeconds(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_seconds" -> $v)"""

  def Second(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("second" -> $v)"""

  def Minute(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("minute" -> $v)"""

  def Hour(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("hour" -> $v)"""

  def DayOfMonth(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("day_of_month" -> $v)"""

  def DayOfWeek(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("day_of_week" -> $v)"""

  def DayOfYear(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("day_of_year" -> $v)"""

  def Month(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("month" -> $v)"""

  def Year(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("year" -> $v)"""

  def DateF(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("date" -> $v)"""

  def TimeAdd(base: c.Tree, offset: c.Tree, unit: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("time_add" -> $base, "offset" -> $offset, "unit" -> $unit)"""

  def TimeSubtract(base: c.Tree, offset: c.Tree, unit: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("time_subtract" -> $base, "offset" -> $offset, "unit" -> $unit)"""

  def TimeDiff(start: c.Tree, finish: c.Tree, unit: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("time_diff" -> $start, "other" -> $finish, "unit" -> $unit)"""

  def ToString(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_string" -> $v)"""

  def ToNumber(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_number" -> $v)"""

  def ToDouble(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_double" -> $v)"""

  def ToInteger(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_integer" -> $v)"""

  def ToTime(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_time" -> $v)"""

  def ToDate(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_date" -> $v)"""

  def ToObject(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_object" -> $v)"""

  def ToArray(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("to_array" -> $v)"""

  def IsNumber(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_number" -> $v)"""

  def IsDouble(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_double" -> $v)"""

  def IsInteger(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_integer" -> $v)"""

  def IsBoolean(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_boolean" -> $v)"""

  def IsNull(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_null" -> $v)"""

  def IsBytes(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_bytes" -> $v)"""

  def IsTimestamp(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_timestamp" -> $v)"""

  def IsDate(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_date" -> $v)"""

  def IsUUID(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_uuid" -> $v)"""

  def IsString(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_string" -> $v)"""

  def IsArray(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_array" -> $v)"""

  def IsObject(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_object" -> $v)"""

  def IsRef(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_ref" -> $v)"""

  def IsSet(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_set" -> $v)"""

  def IsDoc(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_doc" -> $v)"""

  def IsLambda(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_lambda" -> $v)"""

  def IsCollection(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_collection" -> $v)"""

  def IsDatabase(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_database" -> $v)"""

  def IsIndex(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_index" -> $v)"""

  def IsFunction(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_function" -> $v)"""

  def IsKey(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_key" -> $v)"""

  def IsToken(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_token" -> $v)"""

  def IsCredentials(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_credentials" -> $v)"""

  def IsRole(v: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("is_role" -> $v)"""
}

class QueryHelpersImpl27(oc: whitebox.Context) extends QueryHelpersImpl(oc) {
  import c.universe._

  override def RefV2(id: c.Tree, cls: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" ->
      _root_.fauna.codex.json.JSObject("id" -> $id, "collection" -> $cls))"""

  override def RefV3(id: c.Tree, cls: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("@ref" ->
      _root_.fauna.codex.json.JSObject("id" -> $id, "collection" -> $cls, "database" -> $scope))"""

  override def ClassRef1(name: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("collection" -> $name)"""

  override def ClassRef2(name: c.Tree, scope: c.Tree) =
    q"""_root_.fauna.codex.json.JSObject("collection" -> $name, "scope" -> $scope)"""

}
