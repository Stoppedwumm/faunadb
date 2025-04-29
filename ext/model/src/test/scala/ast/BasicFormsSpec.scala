package fauna.model.test

import fauna.ast._
import fauna.atoms.APIVersion
import fauna.auth.Auth
import fauna.lang.Timestamp
import fauna.prop.Generators

class BasicFormsSpec extends Spec with ASTHelpers with Generators {

  lazy val scope = newScope
  lazy val auth = Auth.forScope(scope)

  "if" - {
    "if with true condition returns value of then clause" in {
      val input = ObjectL("if" -> TrueL, "then" -> LongL(1), "else" -> LongL(2))

      run(auth, input) should equal(Right(LongL(1)))
    }

    "if with false condition returns value of else clause" in {
      val input = ObjectL("if" -> FalseL, "then" -> LongL(1), "else" -> LongL(2))

      run(auth, input) should equal(Right(LongL(2)))
    }

    "if with non-boolean condition is error" in {
      val input = ObjectL("if" -> LongL(42), "then" -> LongL(1), "else" -> LongL(2))

      run(auth, input) should equal(
        Left(List(
          InvalidArgument(List(Type.Boolean), Type.Integer, RootPosition at "if"))))
    }
  }

  "let, var" - {
    "evals let expr and var ref" in {
      val input1 = let(
        ObjectL("x" -> StringL("foo")),
        ObjectL(
          "concat" ->
            ArrayL(List(varref("x"), StringL("bar")))))

      run(auth, input1) should equal(Right(StringL("foobar")))

      val input2 = let(
        ArrayL(List(ObjectL("x" -> StringL("foo")), ObjectL("y" -> varref("x")))),
        ObjectL(
          "concat" ->
            ArrayL(List(varref("y"), StringL("bar")))))

      run(auth, input2) should equal(Right(StringL("foobar")))
    }

    "inner scope shadows outer" in {
      val input =
        let(ObjectL("x" -> LongL(1)), let(ObjectL("x" -> LongL(2)), varref("x")))

      run(auth, input) should equal(Right(LongL(2)))
    }

    "inner scope defaults to outer" in {
      val input =
        let(ObjectL("x" -> LongL(1)), let(ObjectL("y" -> LongL(2)), varref("x")))

      run(auth, input) should equal(Right(LongL(1)))
    }

    "let allows back-references" in {
      val input =
        let(ObjectL("x" -> StringL("foo"), "y" -> varref("x")), varref("y"))

      run(auth, input) should equal(Right(StringL("foo")))
    }
  }

  "map" - {
    "evals map with lambda" in {
      val input = map(
        lambda(
          "x",
          ObjectL(
            "concat" ->
              ArrayL(List(varref("x"), StringL("baz"))))),
        ArrayL(List(StringL("foo"), StringL("bar"))))

      run(auth, input) should equal(
        Right(ArrayL(List(StringL("foobaz"), StringL("barbaz")))))
    }
  }

  "do" - {
    "empty do is error" in {
      val input = ObjectL("do" -> ArrayL(Nil))

      run(auth, input) should equal(Left(List(EmptyExpr(RootPosition at "do"))))
    }

    "do value is last value" in {
      val input = ObjectL(
        "do" ->
          ArrayL(List(LongL(1), LongL(2), LongL(3))))

      run(auth, input) should equal(Right(LongL(3)))
    }
  }

  "object" - {
    "constructs objects" in {
      val input =
        ObjectL("object" -> ObjectL("foo" -> LongL(1), "bar" -> StringL("two")))

      run(auth, input) should equal(
        Right(ObjectL("foo" -> LongL(1), "bar" -> StringL("two"))))
    }
  }

  "lambda" - {
    def runLambda(input: Literal, vers: APIVersion = APIVersion.Default) =
      run(auth, input, apiVersion = vers) map {
        _.asInstanceOf[LambdaL].renderableLiteral(vers)
      }

    "evaluates to a lambda value" in {
      pendingUntilFixed {
        val input = ObjectL(
          "lambda" -> StringL("y"),
          "expr" ->
            ObjectL(
              "add" ->
                ArrayL(List(LongL(1), LongL(2)))))

        runLambda(input) should equal(
          Right(
            ObjectL(
              "lambda" -> StringL("y"),
              "expr" -> ObjectL("add" -> ArrayL(List(LongL(1), LongL(2)))))))
      }
    }

    "query form parsed includes query label" in {
      val input = ObjectL(
        "query" -> ObjectL(
          "lambda" -> StringL("x"),
          "expr" -> LongL(1)
        )
      )

      parse(auth, input).toOption.get.literal should equal(input)
    }

    "serializes captures" in {
      pendingUntilFixed {
        val input1 = let(
          ObjectL("x" -> ObjectL("add" -> ArrayL(List(LongL(1), LongL(2))))),
          ObjectL("lambda" -> StringL("y"), "expr" -> ArrayL(List(varref("y")))))

        runLambda(input1) should equal(
          Right(
            ObjectL("lambda" -> StringL("y"), "expr" -> ArrayL(List(varref("y"))))))

        val input2 = let(
          ObjectL("x" -> ObjectL("add" -> ArrayL(List(LongL(1), LongL(2))))),
          ObjectL(
            "lambda" -> StringL("y"),
            "expr" -> ArrayL(List(varref("x"), varref("y")))))

        runLambda(input2) should equal(
          Right(
            ObjectL(
              "lambda" -> StringL("y"),
              "expr" -> ObjectL(
                "let" -> ObjectL("x" -> LongL(3)),
                "in" -> ArrayL(List(varref("x"), varref("y")))))))
      }
    }

    "are syntactically equal" in {
      val input1 =
        ObjectL("query" -> ObjectL("lambda" -> StringL("x"), "expr" -> LongL(1)))

      run(auth, ObjectL("equals" -> ArrayL(List(input1, input1)))) should equal(
        Right(TrueL))

      val input2 =
        ObjectL("query" -> ObjectL("lambda" -> StringL("y"), "expr" -> LongL(1)))
      val input3 =
        ObjectL("query" -> ObjectL("lambda" -> StringL("x"), "expr" -> LongL(2)))

      run(auth, ObjectL("equals" -> ArrayL(List(input1, input2)))) should equal(
        Right(FalseL))
      run(auth, ObjectL("equals" -> ArrayL(List(input1, input3)))) should equal(
        Right(FalseL))
    }

    "are equal based on captured closure" in {
      pendingUntilFixed {
        val input1 = let(
          ObjectL("x" -> LongL(1)),
          ObjectL(
            "lambda" -> StringL("y"),
            "in" -> ObjectL("add" -> ArrayL(List(varref("x"), varref("y"))))))

        val input2 = let(
          ObjectL("x" -> LongL(2)), // different captured value.
          ObjectL(
            "lambda" -> StringL("y"),
            "in" -> ObjectL("add" -> ArrayL(List(varref("x"), varref("y"))))))

        val input3 = let(
          ObjectL("x" -> LongL(1), "z" -> LongL(2)), // extra uncaptured value.
          ObjectL(
            "lambda" -> StringL("y"),
            "in" -> ObjectL("add" -> ArrayL(List(varref("x"), varref("y")))))
        )

        run(auth, ObjectL("equals" -> ArrayL(List(input1, input1)))) should equal(
          Right(TrueL))
        run(auth, ObjectL("equals" -> ArrayL(List(input1, input2)))) should equal(
          Right(FalseL))
        run(auth, ObjectL("equals" -> ArrayL(List(input1, input3)))) should equal(
          Right(TrueL))
      }
    }
  }

  "select XXX" - {
    "select returns field value from object" in {
      val input = ObjectL(
        "select" -> StringL("bar"),
        "from" -> ObjectL(
          "object" -> ObjectL("foo" -> LongL(1), "bar" -> StringL("two"))))

      run(auth, input) should equal(Right(StringL("two")))
    }

    "select returns field value from array" in {
      val input = ObjectL(
        "select" -> LongL(2),
        "from" -> ArrayL(List(LongL(0), LongL(1), LongL(2), LongL(3))))

      run(auth, input) should equal(Right(LongL(2)))
    }

    "don't read through ref data" before (APIVersion.Unstable) in { apiVersion =>
      val collName = aName.sample
      run(
        auth,
        ObjectL(
          "create_collection" -> ObjectL(
            "object" -> ObjectL("name" -> StringL(collName)))))

      val doc = {
        val q = ObjectL(
          "create" -> StringL(collName),
          "params" -> ObjectL(
            "object" -> ObjectL(
              "data" -> ObjectL("object" -> ObjectL("value" -> LongL(1)))))
        )

        val rv = run(auth, q).getOrElse(fail())
        rv.asInstanceOf[VersionL].version
      }

      val ref = RefL(scope, doc.id)

      run(
        auth,
        select(StringL("ref"), ref, StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(StringL("default value"))
      run(
        auth,
        select(StringL("ts"), ref, StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(StringL("default value"))
      run(
        auth,
        select(
          ArrayL(List(StringL("data"), StringL("value"))),
          ref,
          StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(StringL("default value"))
    }

    "read through ref data" after (APIVersion.Unstable) in { apiVersion =>
      val collName = aName.sample
      run(
        auth,
        ObjectL(
          "create_collection" -> ObjectL(
            "object" -> ObjectL("name" -> StringL(collName)))))

      val doc = {
        val q = ObjectL(
          "create" -> StringL(collName),
          "params" -> ObjectL(
            "object" -> ObjectL(
              "data" -> ObjectL("object" -> ObjectL("value" -> LongL(1)))))
        )

        val id = run(auth, q).getOrElse(fail()).asInstanceOf[VersionL].version.id

        run(auth, ObjectL("get" -> RefL(scope, id)))
          .getOrElse(fail())
          .asInstanceOf[VersionL]
          .version
      }

      val ref = RefL(scope, doc.id)

      run(
        auth,
        select(StringL("ref"), ref, StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(ref)
      run(
        auth,
        select(StringL("ts"), ref, StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(LongL(doc.ts.validTS.micros))
      run(
        auth,
        select(
          ArrayL(List(StringL("data"), StringL("value"))),
          ref,
          StringL("default value")),
        apiVersion = apiVersion) shouldBe Right(LongL(1))
    }
  }

  "at" - {
    def runAtSnap(auth: Auth, q: Literal, snap: Timestamp) = {
      val ctx0 = ctx.withStaticSnapshotTime(snap)
      try {
        ctx.cacheContext.setRepo(ctx0)
        ctx.cacheContext.invalidateAll()
        val eval =
          EvalContext.write(auth, snap, APIVersion.Default).parseAndEvalTopLevel(q)
        ctx0 ! eval
      } finally {
        ctx.cacheContext.setRepo(ctx)
        ctx.cacheContext.invalidateAll()
      }
    }

    val readClock = ctx.clock

    val cls = ObjectL(
      "create_collection" -> ObjectL("object" -> ObjectL("name" -> StringL("foo"))))

    run(auth, cls).getOrElse(fail())

    val past = readClock.time

    Thread.sleep(10)

    val inst = {
      val q = ObjectL(
        "create" -> ObjectL("collection" -> StringL("foo")),
        "params" -> ObjectL("object" -> ObjectL()))
      val rv = run(auth, q).getOrElse(fail())
      rv.asInstanceOf[VersionL].version
    }

    Thread.sleep(10)

    val later = readClock.time

    val get = ObjectL("get" -> RefL(scope, inst.id))
    val exists = ObjectL("exists" -> RefL(scope, inst.id))

    "reads without At() form XXX" in {
      runAtSnap(auth, get, past) should matchPattern {
        case Left(List(InstanceNotFound(_, _))) =>
      }
      runAtSnap(auth, get, later).isRight should equal(true)
      runAtSnap(auth, exists, past).isRight should equal(true)
      runAtSnap(auth, exists, later).isRight should equal(true)
    }

    val atGet = ObjectL("at" -> TimeL(later), "expr" -> get)
    val atExists = ObjectL("at" -> TimeL(later), "expr" -> exists)

    "reads past query snapshot time are ran at the snapshot time" in {
      runAtSnap(auth, atGet, past) should matchPattern {
        case Left(List(InstanceNotFound(_, _))) =>
      }
      runAtSnap(auth, atGet, later).isRight should equal(true)
      runAtSnap(auth, atExists, past).isRight should equal(true)
      runAtSnap(auth, atExists, later).isRight should equal(true)
    }

    "reads past query snapshot time are rejected" in {
      pendingUntilFixed {
        runAtSnap(auth, atGet, past) should matchPattern {
          case Left(List(InvalidValidReadTime(_, _))) =>
        }
        runAtSnap(auth, atGet, later).isRight should equal(true)
        runAtSnap(auth, atExists, past) should matchPattern {
          case Left(List(InvalidValidReadTime(_, _))) =>
        }
        runAtSnap(auth, atExists, later).isRight should equal(true)
      }
    }

    val nowGet = ObjectL("at" -> ObjectL("now" -> NullL), "expr" -> get)
    val nowExists = ObjectL("at" -> ObjectL("now" -> NullL), "expr" -> exists)

    "reads at Now() are allowed" in {
      runAtSnap(auth, nowGet, past) should matchPattern {
        case Left(List(InstanceNotFound(_, _))) =>
      }
      runAtSnap(auth, nowGet, later).isRight should equal(true)
      runAtSnap(auth, nowExists, past).isRight should equal(true)
      runAtSnap(auth, nowExists, later).isRight should equal(true)
    }
  }

  "and" - {
    "empty and is error" in {
      val input = ObjectL("and" -> ArrayL(Nil))

      run(auth, input, apiVersion = APIVersion.V212) should equal(
        Left(List(EmptyArrayArgument(RootPosition at "and"))))
      run(auth, input, apiVersion = APIVersion.V3) should equal(
        Left(List(EmptyExpr(RootPosition at "and"))))
    }

    "accepts only boolean values" in {
      val input = ObjectL("and" -> ArrayL(List(TrueL, StringL("str"), FalseL)))

      run(auth, input) should equal(
        Left(
          List(
            InvalidArgument(
              List(Type.Boolean),
              Type.String,
              RootPosition at "and" at 1))))
    }

    "is not short-circuited" before (APIVersion.V3) in { apiVersion =>
      val collName = aName.sample
      val create = ObjectL(
        "create_collection" -> ObjectL(
          "object" -> ObjectL("name" -> StringL(collName))))

      val input = ObjectL(
        "and" -> ArrayL(List(FalseL, ObjectL("do" -> ArrayL(List(create, TrueL))))))

      run(auth, input, apiVersion = apiVersion) should equal(Right(FalseL))
      run(
        auth,
        ObjectL(
          "exists" -> ObjectL("collection" -> StringL(collName)))) should equal(
        Right(TrueL))
    }

    "is short-circuited" after (APIVersion.V3) in { apiVersion =>
      val collName = aName.sample
      val create = ObjectL(
        "create_collection" -> ObjectL(
          "object" -> ObjectL("name" -> StringL(collName))))

      val input = ObjectL(
        "and" -> ArrayL(List(FalseL, ObjectL("do" -> ArrayL(List(create, TrueL))))))

      run(auth, input, apiVersion = apiVersion) should equal(Right(FalseL))
      run(
        auth,
        ObjectL(
          "exists" -> ObjectL("collection" -> StringL(collName)))) should equal(
        Right(FalseL))
    }
  }

  "or" - {
    "empty or is error" in {
      val input = ObjectL("or" -> ArrayL(Nil))

      run(auth, input, apiVersion = APIVersion.V212) should equal(
        Left(List(EmptyArrayArgument(RootPosition at "or"))))
      run(auth, input, apiVersion = APIVersion.V3) should equal(
        Left(List(EmptyExpr(RootPosition at "or"))))
    }

    "accepts only boolean values" in {
      val input = ObjectL("or" -> ArrayL(List(FalseL, StringL("str"), FalseL)))

      run(auth, input) should equal(
        Left(
          List(
            InvalidArgument(
              List(Type.Boolean),
              Type.String,
              RootPosition at "or" at 1))))
    }

    "is not short-circuited" before (APIVersion.V3) in { apiVersion =>
      val collName = aName.sample
      val create = ObjectL(
        "create_collection" -> ObjectL(
          "object" -> ObjectL("name" -> StringL(collName))))

      val input = ObjectL(
        "or" -> ArrayL(List(TrueL, ObjectL("do" -> ArrayL(List(create, TrueL))))))

      run(auth, input, apiVersion = apiVersion) should equal(Right(TrueL))
      run(
        auth,
        ObjectL(
          "exists" -> ObjectL("collection" -> StringL(collName)))) should equal(
        Right(TrueL))
    }

    "is short-circuited" after (APIVersion.V3) in { apiVersion =>
      val collName = aName.sample
      val create = ObjectL(
        "create_collection" -> ObjectL(
          "object" -> ObjectL("name" -> StringL(collName))))

      val input = ObjectL(
        "or" -> ArrayL(List(TrueL, ObjectL("do" -> ArrayL(List(create, TrueL))))))

      run(auth, input, apiVersion = apiVersion) should equal(Right(TrueL))
      run(
        auth,
        ObjectL(
          "exists" -> ObjectL("collection" -> StringL(collName)))) should equal(
        Right(FalseL))
    }
  }
}
