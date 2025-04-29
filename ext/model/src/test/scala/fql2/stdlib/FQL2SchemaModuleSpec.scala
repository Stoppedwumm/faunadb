package fauna.model.test

import fauna.model.runtime.fql2.stdlib
import fauna.model.runtime.fql2.stdlib.CollectionCompanion
import fauna.model.runtime.fql2.UserFunction
import fauna.repo.values.Value
import org.scalactic.source.Position

class FQL2SchemaModuleSpec
    extends FQL2StdlibHelperSpec("FQL.SchemaModule", stdlib.Schema)
    with FQL2WithV4Spec {
  val auth = newDB

  "defForIdentifier" - {
    testSig("defForIdentifier(ident: String) => Any")

    def checkLookup(ident: String)(implicit pos: Position) = {
      val doc = evalOk(auth, s"FQL.Schema.defForIdentifier('$ident')") match {
        case d: Value.Doc => d.id
        case v            => fail(s"unexpected value $v")
      }

      val mod = evalOk(auth, ident) match {
        case coll: CollectionCompanion => coll.collID.toDocID
        case fun: UserFunction         => fun.funcID.toDocID
        case v                         => fail(s"unexpected value $v")
      }

      if (doc != mod) {
        fail(s"Global `$ident` was $mod but defForIdentifier() returned $doc")
      }
    }

    "gets collections and functions by name and alias" in {
      evalOk(auth, "Collection.create({ name: 'Foo', alias: 'Bar' })")
      evalOk(auth, "Function.create({ name: 'Baz', alias: 'Qux', body: 'x => x' })")

      checkLookup("Foo")
      checkLookup("Bar")
      checkLookup("Baz")
      checkLookup("Qux")
    }

    "alias name conflicts" in {
      evalV4Ok(
        auth,
        CreateFunction(
          MkObject("name" -> "CFL", "body" -> QueryF(Lambda("x" -> Var("x"))))))
      evalOk(auth, "Function.byName('CFL')!.update({ alias: 'Fun' })")

      evalV4Ok(auth, CreateCollection(MkObject("name" -> "CFL")))

      checkLookup("CFL")
      checkLookup("Fun")
    }
  }
}
