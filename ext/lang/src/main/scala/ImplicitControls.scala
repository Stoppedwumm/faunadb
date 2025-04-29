package fauna.lang

/**
  * Controls on calling context for dangerous or debug-only
  * commands. These implicits are instantiated only in the appropriate
  * calling context as a means of preventing certain functionality
  * from being available in "normal" code.
  *
  */
sealed abstract class ImplicitControls

/**
  * This control provides access to functionality available only to
  * the Admin tool.
  */
class AdminControl extends ImplicitControls

/**
  * This control provides access to functionality available only to
  * the Scala REPL (aka "Console").
  */
class ConsoleControl extends ImplicitControls
