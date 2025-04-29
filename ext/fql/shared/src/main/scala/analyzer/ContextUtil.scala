package fql.analyzer

import fql.ast.{ Name, Span }

trait ContextUtil { self: Context =>
  def findIdent(cursor: Int): Option[Name] = {
    val initial = if (cursorIsOnIdentifier(cursor)) {
      cursor
    } else if (cursorIsImmediatelyAfterIdentifier(cursor)) {
      cursor - 1
    } else {
      return None
    }
    var position = initial
    var identifierEnd = initial
    var identifierStart = initial + 1

    while (position >= 0 && isValidIdentifierContinue(query(position))) {
      // handles cases like `000foo`
      if (isValidIdentifierStart(query(position))) {
        identifierStart = position
      }
      position -= 1
    }
    // handles cases where no identifier start was found like `000|000`
    if (identifierStart > initial) return None

    while (
      identifierEnd < query.length && isValidIdentifierContinue(query(identifierEnd))
    ) {
      identifierEnd += 1
    }

    Some(
      Name(
        query.slice(identifierStart, identifierEnd),
        span(identifierStart, identifierEnd)))
  }

  def span(start: Int, end: Int) = Span(start, end, src)

  /** Handles cases like
    * `|foo`
    * `f|oo bar`
    * `foo |bar`
    */
  private def cursorIsOnIdentifier(cursor: Int) =
    cursor < query.length && isValidIdentifierContinue(query(cursor))

  /** Handles cases like
    * `foo|`
    * `foo| bar`
    */
  private def cursorIsImmediatelyAfterIdentifier(cursor: Int) =
    cursor > 0 && isValidIdentifierContinue(query(cursor - 1))

  private def isValidIdentifierStart(char: Char) =
    (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_'
  private def isValidIdentifierContinue(char: Char) =
    isValidIdentifierStart(char) || (char >= '0' && char <= '9')
}
