package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.codex.json.JSValue
import fauna.codex.json2.JSON
import fauna.lang.Timestamp
import fauna.model._
import fauna.storage._

class RefParserSpec extends Spec {
  import RefParser._
  import RefScope._

  val invalidNames = Seq("events", "self", "_")

  // FIXME: FUZZME
  val validNames = Seq("foo", "$", "foo_", "less%2Fsf", "selfies", "__")

  val nativeClasses =  Seq(
    "keys" -> KeyClassRef,
    "tokens" -> TokenClassRef,
    "credentials" -> CredentialsClassRef)
  val validCustomClasses = validNames map { n => s"classes/$n" -> UserCollectionRef(n, None) }
  val validClasses = nativeClasses ++ validCustomClasses
  val validInstances = validClasses flatMap { case (cls, ref) =>
    Seq(
      s"$cls/123" -> InstanceRef(Right(SubID(123)), ref),
      s"$cls/self" -> SelfRef(ref))
  }

  // These are split out from `nativeClasses` so as not to break legacy parsing tests.
  val nonLegacyNativeClasses = Seq(
    "classes" -> CollectionCollectionRef,
    "indexes" -> IndexClassRef,
    "functions" -> UserFunctionClassRef,
    "roles" -> RoleClassRef,
    "access_providers" -> AccessProviderClassRef
  )

  "LegacyParsing" - {
    import LegacyRefParser._
    "refs" in {
      validNames foreach { db => Ref.parse(s"databases/$db") should equal (Some(DatabaseRef(db, None))) }
      invalidNames foreach { db => Ref.parse(s"databases/$db") should equal (None) }

      Ref.parse("databases") should equal (Some(DatabaseClassRef))
      Path.parse("databases/events") should equal (Some(EventsRef(DatabaseClassRef)))

      // valid classes
      validClasses foreach { case (cls, ref) =>
        Ref.parse(cls) should equal (Some(ref))
      }

      // schema classes

      Ref.parse("keys") should equal (Some(KeyClassRef))
      Ref.parse("classes") should equal (Some(CollectionCollectionRef))
      Ref.parse("indexes") should equal (Some(IndexClassRef))
      Ref.parse("tokens") should equal (Some(TokenClassRef))
      Ref.parse("credentials") should equal (Some(CredentialsClassRef))
      Ref.parse("functions") should equal (Some(UserFunctionClassRef))
      Ref.parse("roles") should equal (Some(RoleClassRef))
      Ref.parse("access_providers") should equal (Some(AccessProviderClassRef))

      Path.parse("keys/events") should equal (Some(EventsRef(KeyClassRef)))
      Path.parse("classes/events") should equal (Some(EventsRef(CollectionCollectionRef)))
      Path.parse("indexes/events") should equal (Some(EventsRef(IndexClassRef)))
      Path.parse("tokens/events") should equal (Some(EventsRef(TokenClassRef)))
      Path.parse("credentials/events") should equal (Some(EventsRef(CredentialsClassRef)))
      Path.parse("functions/events") should equal (Some(EventsRef(UserFunctionClassRef)))
      Path.parse("roles/events") should equal (Some(EventsRef(RoleClassRef)))
      Path.parse("access_providers/events") should equal (Some(EventsRef(AccessProviderClassRef)))

      // invalid classes
      invalidNames foreach { cls => Ref.parse(s"classes/$cls") should equal (None) }

      // instance refs
      validInstances foreach {
        case (inst, ref) => Ref.parse(inst) should equal (Some(ref))
      }

      invalidNames foreach { n =>
        Ref.parse(s"classes/$n/123") should equal (None)
        Ref.parse(s"classes/$n/self") should equal (None)
      }

      // index refs

      validNames foreach { n =>
        Ref.parse(s"indexes/$n") should equal (Some(IndexRef(n, None)))
      }

      invalidNames foreach { n =>
        Ref.parse(s"indexes/$n") should equal (None)
      }

      // function refs

      validNames foreach { n =>
        Ref.parse(s"functions/$n") should equal (Some(UserFunctionRef(n, None)))
      }

      invalidNames foreach { n =>
        Ref.parse(s"functions/$n") should equal (None)
      }

      // role refs

      validNames foreach { n =>
        Ref.parse(s"roles/$n") should equal (Some(RoleRef(n, None)))
      }

      invalidNames foreach { n =>
        Ref.parse(s"roles/$n") should equal (None)
      }

      // access providers ref

      validNames foreach { n =>
        Ref.parse(s"access_providers/$n") should equal (Some(AccessProviderRef(n, None)))
      }

      invalidNames foreach { n =>
        Ref.parse(s"access_providers/$n") should equal (None)
      }
    }

    "object events timeline refs" in {
      // classes
      validClasses foreach { case (cls, ref) =>
        Path.parse(s"$cls/events") should equal (Some(EventsRef(ref)))
      }

      // instances
      validInstances foreach { case (inst, ref) =>
        Path.parse(s"$inst/events") should equal (Some(EventsRef(ref)))
      }
    }

    "object event refs" in {
      val createID = VersionID(Timestamp.ofMicros(123), Create)
      val deleteID = VersionID(Timestamp.ofMicros(456), Delete)

      // classes
      validClasses foreach { case (cls, ref) =>
        Path.parse(s"$cls/events/123/create") should equal (Some(EventRef(ref, createID)))
        Path.parse(s"$cls/events/456/delete") should equal (Some(EventRef(ref, deleteID)))
      }

      // instances
      validInstances foreach { case (inst, ref) =>
        Path.parse(s"$inst/events/123/create") should equal (Some(EventRef(ref, createID)))
        Path.parse(s"$inst/events/456/delete") should equal (Some(EventRef(ref, deleteID)))
      }
    }

    "is strict" in {
      Seq(
        "/account/databases/fauna-ruby-test2/classes/message_boards/65126420581449728/sets/posts",
        "account/databases/fauna-ruby-test2/classes/message_boards/65126420581449728/sets/posts"
      ) foreach {
        Path.parse(_) should equal (None)
      }
    }
  }

  "Parsing" - {
    import fauna.prop.api.{ DefaultQueryHelpers => Q }

    def toLiteral(js: JSValue): Literal =
      JSON.parse[Literal](js.toString.getBytes("UTF-8"))

    "scoped native references" in {
      nativeClasses ++ nonLegacyNativeClasses foreach { case (name, cls) =>
        val ref = toLiteral(Q.RefV(name, Option.empty[JSValue], Some(Q.DBRefV("DB"))))
        val refR = NativeCollectionRef(cls.collectionID, Some(DatabaseRef("DB", None)))
        RefParser.parse(ref, RootPosition) should equal(Right(refR))
      }

      // IDs that are not native classes should complain for lack of a classref
      validNames foreach { name =>
        val ref = toLiteral(Q.RefV(name, Option.empty[JSValue], Some(Q.DBRefV("DB"))))
        val err = List(InvalidNativeClassRefError(name, RootPosition at "id"))
        RefParser.parse(ref, RootPosition) should equal(Left(err))
      }
    }

    "recursive refs" in {
      val ref = toLiteral(Q.RefV(
        123456789,
        Q.ClsRefV("childClass",
          Q.DBRefV("childDB",
            Q.DBRefV("parentDB")))))

      val refR = InstanceRef(
        Right(SubID(123456789)),
        UserCollectionRef("childClass", Some(
          DatabaseRef("childDB", Some(
            DatabaseRef("parentDB", None))))))

      RefParser.parse(ref, RootPosition) should equal (Right(refR))

      // We don't (yet) allow for non-numeric string ref IDs outside of internal class instances
      val strRef = toLiteral(Q.RefV(
        "string id",
        Q.ClsRefV("childClass",
          Q.DBRefV("childDB",
            Q.DBRefV("parentDB")))))

      RefParser.parse(strRef, RootPosition) should equal (Left(List(
        NonNumericSubIDArgument("string id", RootPosition at "id"))))

      // refs can't be larger than a long
      val tooBigRef = toLiteral(Q.RefV(
        "9223372036854775808", // Long.MaxValue + 1
        Q.ClsRefV("childClass",
          Q.DBRefV("childDB",
            Q.DBRefV("parentDB")))))

      RefParser.parse(tooBigRef, RootPosition) should equal (Left(List(
        SubIDArgumentTooLarge("9223372036854775808", RootPosition at "id"))))

      // can't apply scope to an instance ref
      val scopedInstRef = toLiteral(Q.RefV(123, Some(Q.ClsRefV("cls")), Some(Q.DBRefV("db"))))
      RefParser.parse(scopedInstRef, RootPosition) should equal (Left(List(
        InvalidArgument(List(Type.Null), Type.DatabaseRef, RootPosition at "database"))))

      val idxRef = toLiteral(Q.IdxRefV("my-idx", Q.DBRefV("sub-db")))
      val idxRefR = IndexRef("my-idx", Some(DatabaseRef("sub-db", None)))
      RefParser.parse(idxRef, RootPosition) should equal (Right(idxRefR))

      val selfRef = toLiteral(Q.RefV("self", Q.ClsRefV("myclass")))
      val selfRefR = SelfRef(UserCollectionRef("myclass", None))
      RefParser.parse(selfRef, RootPosition) should equal (Right(selfRefR))
    }

    "invalid classes" in {
      val numClsRef = toLiteral(Q.RefV(1234567890))
      RefParser.parse(numClsRef, RootPosition) should equal (Left(List(InvalidArgument(List(Type.String),Type.Number,RootPosition at "id"))))

      val strClsRef = toLiteral(Q.RefV("1234567890"))
      RefParser.parse(strClsRef, RootPosition) should equal (Left(List(InvalidNativeClassRefError("1234567890", RootPosition at "id"))))
    }
  }

  "nullable casts" - {
    "cast to string" in {
      val str = Casts.String

      // Inference finds an Object here, disregard.
      @annotation.nowarn
      val strOrNull = Casts.Nullable(str)

      str(LongL(123), RootPosition)     should equal(Left(List(InvalidArgument(List(Type.String), Type.Integer, RootPosition))))
      str(DoubleL(123), RootPosition)   should equal(Left(List(InvalidArgument(List(Type.String), Type.Double, RootPosition))))
      str(NullL, RootPosition)          should equal(Left(List(InvalidArgument(List(Type.String), Type.Null, RootPosition))))
      str(StringL("foo"), RootPosition) should equal(Right("foo"))

      strOrNull(LongL(123), RootPosition)     should equal(Left(List(InvalidArgument(List(Type.String, Type.Null), Type.Integer, RootPosition))))
      strOrNull(DoubleL(123), RootPosition)   should equal(Left(List(InvalidArgument(List(Type.String, Type.Null), Type.Double, RootPosition))))
      strOrNull(NullL, RootPosition)          should equal(Right(None))
      strOrNull(StringL("foo"), RootPosition) should equal(Right(Some("foo")))
    }
  }
}
