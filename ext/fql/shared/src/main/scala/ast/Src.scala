package fql.ast

sealed trait Src extends Ordered[Src] {
  def name: String

  def isQuery: Boolean = name == Src.QueryName
  def isNull: Boolean = name == Src.Null.name

  override def equals(other: Any) =
    other match {
      case o: Src => name == o.name
      case _      => false
    }
  override def hashCode = name.hashCode

  override def compare(other: Src): Int = {
    val thisMeta = name.startsWith("*")
    val otherMeta = other.name.startsWith("*")

    // sort meta-sources first
    if (thisMeta && !otherMeta) return -1
    if (!thisMeta && otherMeta) return 1

    name.compare(other.name)
  }
}

object Src {
  final case class Id(name: String) extends Src
  final case class Inline(name: String, contents: String) extends Src

  // SrcIds with names in stars are virtual sources which do represent an .fsl file

  // A SrcId which represents no file
  val Null = Id("*null*")

  // A SrcId that represents it came from decoding expressions
  // this happens with set pagination tokens
  val DecodedSet = Id("*decoded_set*")

  // SrcId representing the query. NB *query*'s contents are a raw FQL expr, not FSL
  val QueryName: String = "*query*"

  // SrcId representing an FSL snippet.
  val FSLName: String = "*fsl*"

  def Query(query: String): Inline = Inline(QueryName, query)
  def FSL(snippet: String): Inline = Inline(FSLName, snippet)

  // SrcId for the UDFs bodies
  object UserFunc {
    def apply(name: String) = Id(s"*udf:$name*")

    private val regex = raw"\*udf:(.+)\*".r
    def unapply(id: Src): Option[String] =
      id match {
        case Id(regex(name)) => Some(name)
        case _               => None
      }
  }

  // SrcId for the predicate bodies
  object Predicate {
    def apply(name: String) = Id(s"*predicate:$name*")

    def apply(name: Option[String]) =
      name.map(n => Id(s"*predicate:$n*")).getOrElse(Id("*predicate*"))

    private val regex = raw"\*predicate:(.+)\*".r
    def unapply(id: Src): Option[String] =
      id match {
        case Id(regex(name)) => Some(name)
        case _               => None
      }
  }

  object SourceFile {
    def apply(filename: String) = Id(filename)

    def unapply(id: Src): Option[String] =
      id match {
        case Id(name) if !name.startsWith("*") => Some(name)
        case _                                 => None
      }
  }
}
