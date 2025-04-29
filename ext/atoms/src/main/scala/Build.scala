package fauna.atoms

import java.util.Properties

object Build {
  private val buildProperties = {
    val prop = new Properties
    val is = getClass.getClassLoader.getResourceAsStream("build.properties")
    Option(is) foreach { i =>
      prop.load(i)
      i.close()
    }
    prop
  }

  private val buildID =
    Option(buildProperties.getProperty("build.id"))

  private val buildVersion =
    Option(buildProperties.getProperty("build.version"))

  def identifier = {
    val identifierOpt = for {
      id <- buildID
      version <- buildVersion
    } yield s"$version-$id"

    identifierOpt getOrElse "unknown"
  }
}
