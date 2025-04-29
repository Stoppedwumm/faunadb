package fauna.model.test

import fauna.ast._
import fauna.atoms._
import fauna.auth.{ Auth, RootAuth }
import fauna.lang.Timestamp
import fauna.model.{ LegacyRefParser, RefParser }
import fauna.repo.query.QDone
import fauna.util.Base64
import io.netty.buffer.Unpooled
import java.time.LocalDate
import java.util.UUID

class ExtendedTypesSpec extends Spec with ASTHelpers {

  lazy val scope = newScope
  lazy val auth = Auth.forScope(scope)

  "@ref" - {
    "parses a legacy ref" in {
      val q = ObjectL("@ref" -> StringL("keys"))

      parseEscapes(auth, q) should equal (Right(RefL(scope, KeyID.collID)))
      parse(auth, q) should equal (Right(RefL(scope, KeyID.collID)))
    }

    "fails with InvalidRef if string is invalid" in {
      val q = ObjectL("@ref" -> StringL("foo"))

      parseEscapes(auth, q) should equal (Left(List(InvalidRefExpr(Pos()))))
      parse(auth, q) should equal (Left(List(InvalidRefExpr(Pos()))))
    }

    "parses as an unresolved ref if ref is not found" in {
      Seq(
        "databases/nonexistent",
        "indexes/nonexistent",
        "classes/nonexistent",
        "classes/nonexistent/123"
      ) foreach { refStr =>
        val q = ObjectL("@ref" -> StringL(refStr))
        val ref = LegacyRefParser.Ref.parse(refStr).get
        parseEscapes(auth, q) should equal(Right(UnresolvedRefL(ref)))
        parse(auth, q) should equal(Right(UnresolvedRefL(ref)))
      }
    }

    "fails if not passed a string or object" in {
      val q = ObjectL("@ref" -> LongL(1))
      parseEscapes(auth, q) should equal (Left(List(InvalidRefExpr(Pos()))))
      parse(auth, q) should equal (Left(List(InvalidRefExpr(Pos()))))
    }

    "parses a scoped ref" in {
      val v2Scope = newScope(RootAuth, "my-database", APIVersion.V21)
      val v2auth = Auth.forScope(v2Scope)
      val subDB = newScope(Auth.adminForScope(v2Scope), "my-child-database", APIVersion.V21)
      val cls = run(Auth.forScope(subDB), ObjectL(
        "create" -> ObjectL("@ref" -> ObjectL("id" -> StringL("classes"))),
        "params" -> ObjectL("object" -> ObjectL("name" -> StringL("my-custom-class")))))

      val clsID = cls match {
        case Right(VersionL(v, _)) => v.id.as[CollectionID]
        case _ => sys.error(s"Unexpected: $cls")
      }

      val q = ObjectL("@ref" -> ObjectL(
        "id" -> LongL(123456789),
        "class" -> ObjectL("@ref" -> ObjectL(
          "id" -> StringL("my-custom-class"),
          "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("classes"))),
          "database" -> ObjectL("@ref" -> ObjectL(
            "id" -> StringL("my-child-database"),
            "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("databases")))))))))

      val ref = RefL(subDB, DocID(SubID(123456789), clsID))
      parseEscapes(v2auth, q) should equal (Right(ref))
      parse(v2auth, q) should equal (Right(ref))
    }

    "parses as an unresolved ref if the scope is not found" in {
      val q = ObjectL("@ref" -> ObjectL(
        "id" -> LongL(123456789),
        "class" -> ObjectL("@ref" -> ObjectL(
          "id" -> StringL("my-custom-class"),
          "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("classes"))),
          "database" -> ObjectL("@ref" -> ObjectL(
            "id" -> StringL("my-child-database"),
            "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("databases")))))))))

      val ref = RefParser.parse(q, RootPosition).getOrElse(fail())
      parseEscapes(auth, q) should equal (Right(UnresolvedRefL(ref)))
      parse(auth, q) should equal (Right(UnresolvedRefL(ref)))
    }

    "parses as an unresolved ref if the scope is not within the key scope" in {
      val v2Scope = newScope(RootAuth, "my-database-2", APIVersion.V21)
      newScope(Auth.adminForScope(v2Scope), "my-child-database-2", APIVersion.V21)
      val v2OutsideScope = newScope(RootAuth, "my-database-3", APIVersion.V21)
      val v2OutsideAuth = Auth.forScope(v2OutsideScope)

      val q = ObjectL("@ref" -> ObjectL(
        "id" -> LongL(123456789),
        "class" -> ObjectL("@ref" -> ObjectL(
          "id" -> StringL("my-custom-class-2"),
          "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("classes"))),
          "database" -> ObjectL("@ref" -> ObjectL(
            "id" -> StringL("my-child-database-2"),
            "class" -> ObjectL("@ref" -> ObjectL("id" -> StringL("databases")))))))))

      val ref = RefParser.parse(q, RootPosition).getOrElse(fail())
      parseEscapes(v2OutsideAuth, q) should equal (Right(UnresolvedRefL(ref)))
      parse(v2OutsideAuth, q) should equal (Right(UnresolvedRefL(ref)))
    }
  }

  "@obj" - {
    "no-op with normal keys" in {
      val q = ObjectL("@obj" -> ObjectL("foo" -> StringL("bar")))

      parseEscapes(auth, q) should equal (Right(ObjectL("foo" -> StringL("bar"))))
    }

    "allows creation of an object with @ keys" in {
      val q1 = ObjectL("@obj" -> ObjectL("@ref" -> StringL("foo")))
      val q2 = ObjectL("@obj" -> ObjectL("@non" -> StringL("foo")))

      parseEscapes(auth, q1) should equal (Right(ObjectL("@ref" -> StringL("foo"))))
      parseEscapes(auth, q2) should equal (Right(ObjectL("@non" -> StringL("foo"))))
    }

    "behaves well with object" in {
      parse(auth, ObjectL("object" -> ObjectL("@obj" -> ObjectL("@ref" -> StringL("foo"))))) should equal (Right(ObjectE(List("@ref" -> StringL("foo")))))
      parse(auth, ObjectL("@obj" -> ObjectL("object" -> ObjectL("@ref" -> StringL("foo"))))) should equal (Left(List(InvalidRefExpr(Pos("@obj", "object")))))
    }

    "behaves well with let" in {
      parse(auth, ObjectL("let" -> ObjectL("@obj" -> ObjectL("@ref" -> StringL("bar"))), "in" -> ObjectL("var" -> StringL("@ref")))) should equal (Right(
        LetE(List("@ref" -> StringL("bar")), VarE("@ref"))))
    }
  }

  "@set" - {
    val clsRef = run(RootAuth, ObjectL("create_class" -> ObjectL("object" -> ObjectL("name" -> StringL("foo"))))) match {
      case Right(VersionL(v, _)) => RefL(v.parentScopeID, v.id)
      case _ => throw new RuntimeException("Class creation should have worked!")
    }

    "is what is returned by requests for events (RefL)" in {
      val r = EventsFunction(clsRef, RootPosition) match {
        case QDone(Right(s)) => s
        case _ => throw new RuntimeException("SetL should be able to be extracted from a class ref!")
      }

      run(RootAuth, r) should matchPattern { case Right(SetL(_)) => }
    }

    "is what is returned by requests for events (EventSet)" - {
      val idx = run(RootAuth, ObjectL("create_index" -> ObjectL("object" ->
        ObjectL(
          "name" -> StringL("fooIdx"),
          "active" -> BoolL(true),
          "source" -> ObjectL("class" -> StringL("foo"))))))
      idx match {
        case Right(VersionL(v, true)) if v.parentScopeID == RootAuth.scopeID => ()
        case _ => throw new RuntimeException("Index creation should have worked!")
      }

      val s = run(RootAuth, ObjectL("match" -> ObjectL("index" -> StringL("fooIdx")))) match {
        case Right(set @ SetL(_)) => set
        case _ => throw new RuntimeException("SetL should be able to be extracted from an EventsFunction expr!")
      }

      val r = EventsFunction(s, RootPosition) match {
        case QDone(Right(s)) => s
        case _ => throw new RuntimeException("SetL should be able to be extracted from an EventsFunction expr!")
      }

      run(RootAuth, r) should matchPattern { case Right(SetL(_)) => }
    }

    "is what is returned by requests for singletons" in {

      val r = SingletonFunction(clsRef, RootPosition) match {
        case QDone(Right(s)) => s
        case _ => throw new RuntimeException("SetL should be able to be extracted from an SingletonFunction expr!")
      }

      run(RootAuth, r) should matchPattern { case Right(SetL(_)) => }
    }
  }

  "@ts" - {
    "parses strings as offset time" in {
      val ts = Timestamp.ofMillis(0)
      val q = ObjectL("@ts" -> StringL("1970-01-01T00:00:00+00:00"))

      parseEscapes(auth, q) should equal (Right(TimeL(ts)))
      parse(auth, q) should equal (Right(TimeL(ts)))
    }
  }

  "@date" - {
    "parses strings as dates" in {
      val date = LocalDate.ofEpochDay(0)
      val q = ObjectL("@date" -> StringL("1970-01-01"))

      parseEscapes(auth, q) should equal (Right(DateL(date)))
      parse(auth, q) should equal (Right(DateL(date)))
    }
  }

  "@query" - {
    def lambdaLit(l: LambdaL, vers: APIVersion) =
      l.renderableLiteral(vers)

    "parses as to a LambdaL" in {
      val expr = ObjectL("lambda" -> StringL("y"), "expr" -> ObjectL("var" -> StringL("y")))
      val q = ObjectL("@query" -> expr)

      val expected = Right(ObjectL(
        "lambda" -> StringL("y"),
        "expr" -> ObjectL("var" -> StringL("y"))))

      val res1 = parseEscapes(auth, q)
      val res2 = parse(auth, q)

      res1 map { l => lambdaLit(l.asInstanceOf[LambdaL], APIVersion.Default) } should equal (expected)
      res2 map { l => lambdaLit(l.asInstanceOf[LambdaL], APIVersion.Default) } should equal (expected)
    }

    "correctly renders, parses versioned lambdas" in {
      def mkExpr(fn: String) =
        ObjectL(
          fn -> StringL("foo"),
          "in" -> ObjectL("object" -> ObjectL()))

      val api2Lambda = ObjectL("lambda" -> StringL("_"), "expr" -> mkExpr("contains"))
      val versionedAPI2Lambda = ObjectL(("api_version" -> StringL("2.12")) :: api2Lambda.elems)

      val api3Lambda = ObjectL("lambda" -> StringL("_"), "expr" -> mkExpr("contains_field"))
      val versionedAPI3Lambda = ObjectL(("api_version" -> StringL("3")) :: api3Lambda.elems)

      // API 2 lambdas get a version tag when rendered with 3+

      val res2 = Seq(
        run(auth, ObjectL("query" -> api2Lambda), apiVersion = APIVersion.V212),

        parseEscapes(auth, ObjectL("@query" -> api2Lambda), apiVersion = APIVersion.V212),
        parse(auth, ObjectL("@query" -> api2Lambda), apiVersion = APIVersion.V212),
        parseEscapes(auth, ObjectL("@query" -> api2Lambda), apiVersion = APIVersion.V3),
        parse(auth, ObjectL("@query" -> api2Lambda), apiVersion = APIVersion.V3),

        parseEscapes(auth, ObjectL("@query" -> versionedAPI2Lambda), apiVersion = APIVersion.V212),
        parse(auth, ObjectL("@query" -> versionedAPI2Lambda), apiVersion = APIVersion.V212),
        parseEscapes(auth, ObjectL("@query" -> versionedAPI2Lambda), apiVersion = APIVersion.V3),
        parse(auth, ObjectL("@query" -> versionedAPI2Lambda), apiVersion = APIVersion.V3))

      res2 map {
        _.toOption.get.asInstanceOf[LambdaL]
      } foreach { l =>
        lambdaLit(l, APIVersion.V21) should equal (api2Lambda)
        lambdaLit(l, APIVersion.V212) should equal (api2Lambda)
        lambdaLit(l, APIVersion.V3) should equal (versionedAPI2Lambda)
      }

      // API 3+ lambdas get a version tag always

      val res3 = Seq(
        run(auth, ObjectL("query" -> api3Lambda), apiVersion = APIVersion.V3),

        parseEscapes(auth, ObjectL("@query" -> versionedAPI3Lambda), apiVersion = APIVersion.V212),
        parse(auth, ObjectL("@query" -> versionedAPI3Lambda), apiVersion = APIVersion.V212),
        parseEscapes(auth, ObjectL("@query" -> versionedAPI3Lambda), apiVersion = APIVersion.V3),
        parse(auth, ObjectL("@query" -> versionedAPI3Lambda), apiVersion = APIVersion.V3))

      res3 map {
        _.toOption.get.asInstanceOf[LambdaL]
      } foreach { l =>
        lambdaLit(l, APIVersion.V21) should equal (versionedAPI3Lambda)
        lambdaLit(l, APIVersion.V212) should equal (versionedAPI3Lambda)
        lambdaLit(l, APIVersion.V3) should equal (versionedAPI3Lambda)
      }

      // older API versions render as 2.12

      val apiOldRes = run(auth, ObjectL("query" -> api2Lambda), apiVersion = APIVersion.V21)

      apiOldRes map { l => lambdaLit(l.asInstanceOf[LambdaL], APIVersion.V21) } should equal (Right(api2Lambda))
      apiOldRes map { l => lambdaLit(l.asInstanceOf[LambdaL], APIVersion.V212) } should equal (Right(api2Lambda))
      apiOldRes map { l => lambdaLit(l.asInstanceOf[LambdaL], APIVersion.V3) } should equal (Right(versionedAPI2Lambda))
    }
  }

  "@bytes" - {
    "parses base64 string as bytes" in {
      val bytes = Array[Byte](0x1, 0x2, 0x3, 0x4)
      val buf = Unpooled.wrappedBuffer(bytes)
      val q = ObjectL("@bytes" -> StringL(Base64.encodeUrlSafe(bytes)))
      parseEscapes(auth, q) should equal (Right(BytesL(buf)))
      parse(auth, q) should equal (Right(BytesL(buf)))
    }
  }

  "@uuid" - {
    "parses strings as UUIDs" in {
      val id = UUID.randomUUID
      val q = ObjectL("@uuid" -> StringL(id.toString))

      parseEscapes(auth, q) should equal (Right(UUIDL(id)))
      parse(auth, q) should equal (Right(UUIDL(id)))
    }
  }
}
