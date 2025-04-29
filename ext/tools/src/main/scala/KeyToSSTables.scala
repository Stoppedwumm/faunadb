package fauna.tools

import org.apache.commons.cli.Options

object KeyToSSTables extends SSTableApp("KeyToSSTables") {

  override def setPerAppCLIOptions(o: Options) = {
    o.addOption("n", "cf", true, "Column Family")
    o.addOption("k", "key", true, "key")
  }

  start {
    val key = cli.getOptionValue("k")
    val cf = cli.getOptionValue("n")

    if (cf == null || key == null) {
      handler.output("Column Family and Key are necessary")
      sys.exit(1)
    }

    handler.output(s"Files for $key")
    openColumnFamily(cf, withSSTables = true)
      .getSSTablesForKey(key)
      .forEach(sstable => handler.output(sstable))
    sys.exit(0)
  }
}
