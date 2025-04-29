package fql.parser

import fastparse._

object Tokens {
  val reservedTypeNames = Set(
    "Any",
    "Never",
    "struct",
    "interface",
    "type",
    "function",
    "collection"
  )
  // Used to disallow idents as keywords.
  val invalidIdents =
    Set(
      "if",
      "else",
      "at",
      "match",
      "case",
      "let",
      "null",
      "true",
      "false",
      "isa",
      "join",
      "on",
      "of"
    )
  // Used to disallow certain variable names. The way this is used means it doesn't
  // need to include all `invalidIdents`, but for clarity I think it's best to just
  // make this the set of all invalid variable names.
  val invalidVariables = invalidIdents ++ Set("FQL", "_")

  val ValidIdRegex = "[a-zA-Z_][a-zA-Z0-9_]*".r

  def isValidIdent(s: String) =
    !invalidIdents.contains(s) && ValidIdRegex.matches(s)

  // Field names reserved by the model, but here because it helps parsing.
  val ReservedFieldNames = Set(
    // v10 meta fields (intentionally does not include ttl because it's mutable).
    "id",
    "coll",
    "ts",
    "data",
    // Document method names.
    "delete",
    "update",
    "replace",
    "updateData",
    "replaceData",
    "exists"
  )

  // Special field names are names of user fields that are rendered inside "data"
  // because they conflict with special names.
  val SpecialFieldNames = ReservedFieldNames + "ttl"
}

@annotation.nowarn("msg=Top-level wildcard*")
/** Tokens used by FQL-X/FSL. */
trait Tokens { parser: Parser =>
  // Expression initiating keywords.
  def `if`[_: P] = P(KW("if") ~~/ nl)
  def `at`[_: P] = P(KW("at") ~~/ nl)
  def `else`[_: P] = P(KW("else") ~~/ nl)
  def `match`[_: P] = P(KW("match") ~~/ nl)
  def `case`[_: P] = P(KW("case") ~~/ nl)
  def `let`[_: P] = P(KW("let") ~~/ nl)

  def `:`[_: P] = P(":" ~~/ nl).opaque("`:`")
  def `@`[_: P] = P("@" ~~/ nl).opaque("`@`")
  def `=`[_: P] = P("=" ~~/ nl).opaque("`=`")
  def `,`[_: P] = P(
    "," ~~ nl
  ).opaque("`,`") // cannot cut on comma, or backing out of a repetition fails
  def `.`[_: P] = P("." ~~/ nl).opaque("`.`")
  def `dotNoCut`[_: P] = P("." ~~ nl).opaque("`.`")
  def `?.`[_: P] = P("?." ~~/ nl).opaque("`?.`")
  def `...`[_: P] = P("..." ~~/ nl).opaque("`...`")
  def `{`[_: P] = P("{" ~~/ nl).opaque("`{`")
  def `(`[_: P] = P("(" ~~/ nl).opaque("`(`")
  def `[`[_: P] = P("[" ~~/ nl).opaque("`[`")
  def `<`[_: P] = P("<" ~~/ nl).opaque("`<`")
  def `=>`[_: P] = P("=>" ~~/ nl).opaque("`=>`")
  def `->`[_: P] = P("->" ~~/ nl).opaque("`->`")
  def `*`[_: P] = P("*" ~~/ nl).opaque("`*`")

  def `}`[_: P] = P("}"./).opaque("`}`")
  def `)`[_: P] = P(")"./).opaque("`)`")
  def `]`[_: P] = P("]"./).opaque("`]`")
  def `>`[_: P] = P(">"./).opaque("`>`")
  def `_`[_: P] = P("_"./).opaque("`_`")
  def `!`[_: P] = P("!"./).opaque("`!`")
  def `?`[_: P] = P("?"./).opaque("`?`")

  // Cannot have a cut here, so that we can backtrack for object/block cases.
  def `"`[_: P] = P("\"").opaque("`\"`")
  def `<<+`[_: P] = P("<<+"./).opaque("`<<+`")
  def `<<-`[_: P] = P("<<-"./).opaque("`<<-`")

  def `[]`[_: P] = P("[]" ~~/ nl)

  def `null`[_: P] = KW("null")./
  def `Null`[_: P] = KW("Null")./
  def `true`[_: P] = KW("true")./
  def `false`[_: P] = KW("false")./

  def `Any`[_: P] = KW("Any")./
  def `Never`[_: P] = KW("Never")./

  def `collection`[_: P] = P(KW("collection") ~~/ nl)
  def `struct`[_: P] = P(KW("struct") ~~/ nl)
  def `interface`[_: P] = P(KW("interface") ~~/ nl)
  def `function`[_: P] = P(KW("function") ~~/ nl)
  def `type`[_: P] = P(KW("type") ~~/ nl)

  def `implements`[_: P] = P(KW("implements") ~~/ nl)
  def `extends`[_: P] = P(KW("extends") ~~/ nl)
  def `static`[_: P] = P(KW("static") ~~/ nl)
  def `validate`[_: P] = P(KW("validate") ~~/ nl)
  def `unique`[_: P] = P(KW("unique") ~~/ nl)
  def `index`[_: P] = P(KW("index") ~~/ nl)
  def `terms`[_: P] = P(KW("terms") ~~/ nl)
  def `values`[_: P] = P(KW("values") ~~/ nl)
  def `role`[_: P] = P(KW("role") ~~/ nl)
  def `privileges`[_: P] = P(KW("privileges") ~~/ nl)
  def `membership`[_: P] = P(KW("membership") ~~/ nl)
  def `access`[_: P] = P(KW("access") ~~/ nl)
  def `provider`[_: P] = P(KW("provider") ~~/ nl)
  def `access_provider`[_: P] = P(KW("access provider") ~~/ nl)
  def `issuer`[_: P] = P(KW("issuer") ~~/ nl)
  def `jwks_uri`[_: P] = P(KW("jwks_uri") ~~/ nl)
  def `predicate`[_: P] = P(KW("predicate") ~~/ nl)
  def `history_days`[_: P] = P(KW("history_days") ~~/ nl)
  def `ttl_days`[_: P] = P(KW("ttl_days") ~~/ nl)
  def `alias`[_: P] = P(KW("alias") ~~/ nl)
  def `compute`[_: P] = P(KW("compute") ~~/ nl)
}
