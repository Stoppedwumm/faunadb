package fql

// A (temporary?) home for text utils.
object TextUtil {

  def diff(actual: String, expected: String) = {
    val sb = new StringBuilder
    printDiff(actual, expected, sb)
    sb.result()
  }

  // Writes the line-by-line diff of the two strings to `sb`.
  def printDiff(actual: String, expected: String, sb: StringBuilder) = {
    def remove(line: String) = if (line.isEmpty) {
      sb.append(s"-\n")
    } else {
      sb.append(s"- $line\n")
    }
    def add(line: String) = if (line.isEmpty) {
      sb.append(s"+\n")
    } else {
      sb.append(s"+ $line\n")
    }

    val actualLines = actual.linesIterator.toSeq
    val expectedLines = expected.linesIterator.toSeq
    var a = 0
    var e = 0
    def nextMatch(): (Int, Int) = {
      var a0 = a + 1
      var e0 = e + 1
      // find the next matching line in both at the same time
      while (a0 < actualLines.size || e0 < expectedLines.size) {
        var res = Option.empty[(Int, Int)]

        if (a0 < actualLines.size) {
          val actual = actualLines(a0)
          val expectedIndex = expectedLines.drop(e).indexOf(actual)
          if (expectedIndex != -1) {
            res = Some((a0, expectedIndex + e))
          }
        }

        if (e0 < expectedLines.size) {
          val expected = expectedLines(e0)
          val actualIndex = actualLines.drop(a).indexOf(expected)
          if (actualIndex != -1) {
            val out = (actualIndex + a, e0)
            // if we get two matches, then choose the shorter one
            return res match {
              case Some(other) => if (other._1 < out._1) other else out
              case None        => out
            }
          }
        }

        a0 += 1
        e0 += 1
      }
      (-1, -1)
    }
    while (a < actualLines.size && e < expectedLines.size) {
      val actual = actualLines(a)
      val expected = expectedLines(e)
      if (actual == expected) {
        if (actual.isEmpty) {
          sb.append(s"\n")
        } else {
          sb.append(s"  $actual\n")
        }
        a += 1
        e += 1
      } else {
        val (nextA, nextE) = nextMatch()
        if (nextA == -1) {
          expectedLines.drop(e).foreach(remove)
          actualLines.drop(a).foreach(add)
          // no more matches, so we finish the loop here
          a = actualLines.size
          e = expectedLines.size
        } else {
          expectedLines.slice(e, nextE).foreach(remove)
          actualLines.slice(a, nextA).foreach(add)
          a = nextA
          e = nextE
        }
      }
    }
    if (e < expectedLines.size) {
      expectedLines.drop(e).foreach(remove)
    }
    if (a < actualLines.size) {
      actualLines.drop(a).foreach(add)
    }
  }
}
