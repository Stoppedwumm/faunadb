package fauna.storage

import java.util.regex.Pattern

object ReplicaNameValidator {
  //  Must start with a letter or number.
  //  May contain only letter, numbers, underscore, dash.
  //  Can consist of two components (region-name/replica-name)
  //  with the same constraints, separated by a slash.
  private final val ReplicaNamepattern =
    Pattern.compile("""^[\p{IsAlphabetic}\d][\p{IsAlphabetic}\d_-]*(/[\p{IsAlphabetic}\d][\p{IsAlphabetic}\d_-]*)?""")

  /**
    * Check for a valid replica name
    * to be valid it must start with a letter and contain
    * only letters, digits dash or underscore
    * @param name  the valid replica name
    * @return true if a valid name is found
    */
  def isValid(name: String): Boolean = {
    name != null && ReplicaNamepattern.matcher(name).matches
  }

  def invalidNameMessage(name: String): String =
    s"""Replica name "$name" must begin with an alphanumeric character and may """ +
      "only contain letters, numbers, dashes, and underscores. It can optionally " +
      "be prefixed with a region name following the same rules and separated " +
      "from the replica name by a slash."
}
