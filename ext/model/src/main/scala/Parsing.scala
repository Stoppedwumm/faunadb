package fauna.model

import java.util.regex.Pattern

object Parsing {

  // Rejected reserved names
  val ReservedNames = Set[CharSequence]("_")

  // Valid URL path segment characters according to RFC2396
  // val Chars = """[;:@&=+$,\-_.!~*'()%\w]+"""
  // minus chars & = ( ) ' ,
  private val Chars = """;@+$\-_.!~%\w"""
  val ValidNameChars = s"""[${Chars}]+"""

  object Name {
    private val pattern =
      Pattern.compile(ValidNameChars, Pattern.UNICODE_CHARACTER_CLASS)

    def validChars(str: CharSequence) = pattern.matcher(str).matches

    def invalidChars(str: CharSequence) = !validChars(str)

    def reserved(str: CharSequence) = ReservedNames.contains(str)

    def isValid(str: CharSequence) = validChars(str) && !reserved(str)

    def unapply(str: CharSequence) = Some(str).filter(isValid)

  }

  object Email {
    val Str = """[\w.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*"""

    private val pattern = Pattern.compile(Str, Pattern.UNICODE_CHARACTER_CLASS)

    def isValid(str: CharSequence) = pattern.matcher(str).matches

    def unapply(str: CharSequence) = Some(str).filter(isValid)
  }
}
