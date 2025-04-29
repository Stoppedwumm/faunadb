package fauna.ast

import fauna.storage.Event

final case class Cursor(literal: CursorL, event: Event)
