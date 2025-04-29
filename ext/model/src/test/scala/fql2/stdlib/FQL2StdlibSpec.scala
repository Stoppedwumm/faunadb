package fauna.model.test

import fauna.auth.Auth
import fauna.codex.json2.JSON
import fauna.model.runtime.fql2.stdlib.Global
import fauna.model.runtime.fql2.BaseFieldTable
import fauna.model.runtime.fql2.FQLInterpreter
import fauna.model.runtime.fql2.NativeFunction
import fauna.model.runtime.fql2.NativeMethod
import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.model.runtime.fql2.QueryRuntimeFailure
import fauna.model.runtime.fql2.ToString._
import fauna.model.runtime.fql2.TypeTag
import fauna.repo.values.Value
import fql.ast.display._
import fql.ast.Name
import fql.ast.Span
import fql.ast.Src
import fql.typer.Type
import fql.typer.Typer
import io.netty.buffer.ByteBufUtil
import java.io.File
import java.io.FileOutputStream
import org.scalactic.source.Position
import scala.collection.mutable.{ Map => MMap }

/** Stores the individual checks for a single test section.
  */
case class Section(name: String, var checks: Seq[(String, String)] = Seq.empty) {
  def beautify(query: String) = {
    query.replace(";", "\n").lines.map { _.trim() }.toArray.mkString("\n")
  }
  def addCheck(query: String, result: String) = {
    checks = checks ++ Seq(beautify(query) -> result)
  }
}

/** Stores all test sections for a single function.
  */
case class FunctionTest(
  function: String,
  var signatures: Seq[String] = Seq.empty,
  var sections: Seq[Section] = Seq.empty) {
  def current = sections.last
  def addSection(name: String) = {
    sections = sections ++ Seq(Section(name))
  }
  def addCheck(query: String, result: String) = {
    current.addCheck(query, result)
  }
}

object Encoders {
  implicit val SectionEncoder = JSON.RecordEncoder[Section]
  implicit val FunctionTestEncoder = JSON.RecordEncoder[FunctionTest]
}

class FQL2StdlibHelperSpec[V <: Value](module: String, proto: BaseFieldTable[V])
    extends FQL2StdlibSpec(module) {

  def testSig(sig: String*)(implicit pos: Position) =
    s"has signature $sig" inWithTest { test =>
      test.signatures = sig
      lookupSig(proto, test.function) shouldBe sig
    }

  def testSigPending(sig: String*)(implicit pos: Position) =
    s"has signature $sig" inWithTest { test =>
      pendingUntilFixed {
        test.signatures = sig
        lookupSig(proto, test.function) shouldBe sig
      }
    }
}

class FQL2StdlibSpec(module: String) extends FQL2Spec {
  val typer = Global.StaticEnv.newTyper()

  val tests = MMap.empty[String, FunctionTest]
  var current = ""

  // All the `-` calls happen first, then all the `in` calls are executed. This means
  // we need to mess around with `current` a bunch.
  implicit class TestOps(name: String) {
    def -(test: => Unit) = {
      convertToFreeSpecStringWrapper(name).- {
        current = name
        tests += name -> FunctionTest(name)
        test
      }
    }

    def in(test: => Unit) = {
      val funcName = current
      convertToFreeSpecStringWrapper(name).in {
        current = funcName
        tests(funcName).addSection(name)
        test
      }
    }

    def inWithTest(test: (FunctionTest) => Unit) = {
      val funcName = current
      convertToFreeSpecStringWrapper(name).in {
        test(tests(funcName))
      }
    }
  }

  def checkOk(auth: Auth, query: String, expected: Value, typecheck: Boolean = true)(
    implicit pos: Position) = {
    tests(current).addCheck(
      query,
      ctx ! expected.toDisplayString(new FQLInterpreter(auth)))
    super.evalOk(auth, query, typecheck) shouldBe expected
  }

  def checkOkApprox(auth: Auth, query: String, expected: Double, tol: Double)(
    implicit pos: Position) = {
    tests(current).addCheck(
      query,
      ctx ! Value.Double(expected).toDisplayString(new FQLInterpreter(auth)))
    super.evalOk(auth, query)(pos) match {
      case Value.Double(v) => v should equal(expected +- tol)
      case _               => fail()(pos)
    }
  }

  def checkErr(auth: Auth, query: String, expected: QueryCheckFailure)(
    implicit pos: Position) = {
    tests(current).addCheck(
      query,
      expected.errors.map { _.renderWithSource(Map.empty) }.mkString("\n\n"))
    super.evalErr(auth, query)(pos) shouldBe expected
  }

  def checkErr(auth: Auth, query: String, expected: QueryRuntimeFailure)(
    implicit pos: Position) = {
    tests(current).addCheck(
      query,
      expected
        .copy(trace = FQLInterpreter.StackTrace(expected.trace.trace.map { span =>
          span.copy(src = Src.Query(query))
        }))
        .renderWithSource(Map.empty))
    super.evalErr(auth, query)(pos) shouldBe expected
  }

  val skipFields = Set("<", "<=", "==", "!=", ">=", ">", "[]")

  def checkAllTested() = {
    val fields =
      typer.typeShapes(module).fields.keys ++ typer.typeShapes(module).ops.keys
    fields.foreach { field =>
      if (!skipFields.contains(field) && !tests.contains(field)) {
        fail(s"function $module.$field is not tested")
      }
    }
  }

  // Export docs with
  // ```
  // FQL_EXPORT_DOCS=true sbt "model / testOnly *FQL2*"
  // ```
  override protected def afterAll(): Unit = {
    checkAllTested()

    if (sys.env.get("FQL_EXPORT_DOCS") != None) {
      import Encoders._
      val encoded = JSON.encode(tests.values.toSeq)
      val bytes = ByteBufUtil.getBytes(encoded);

      new File("target/templates/").mkdirs
      val out = new FileOutputStream(s"target/templates/$module.json")
      out.write(bytes);
      out.close()
    }
  }

  private def displayArg(arg: NativeFunction.NameWithType[_]): String =
    s"${arg.n}: ${Typer().valueToExpr(arg.ty).display}"
  private def displayRet(tt: TypeTag[_]): String =
    if (tt.staticType != Type.Null) {
      s" => ${Typer().valueToExpr(tt.staticType).display}"
    } else {
      ""
    }

  private def displayMethod[V <: Value](m: NativeMethod[V]): Seq[String] = {
    m match {
      case m: NativeMethod.Impl.Overloaded[V] => m.methods.flatMap(displayMethod)
      case m: NativeMethod.Impl.FixedArg[V] =>
        val args = m.params.map(displayArg).mkString(", ")
        val ret = displayRet(m.ret)
        Seq(s"${m.name}($args)$ret")
      case m: NativeMethod.Impl.VarArg[V] =>
        val arg = displayArg(m.param)
        val ret = displayRet(m.ret)
        Seq(s"${m.name}(...$arg)$ret")
    }
  }

  /** Gets the type signature of the given field or op `member` registered for
    * `typeName`.
    */
  def lookupSig[V <: Value](proto: BaseFieldTable[V], member: String)(
    implicit pos: Position): Seq[String] = {
    // access is special
    val sig = if (member == "[]") {
      proto.accessSigComponents.map { case (p, ret) =>
        s"[](${displayArg(p)})${displayRet(ret)}"
      }.toSeq
    } else {
      proto.getFieldOrMethod(member) match {
        case Some(Left(m))         => displayMethod(m)
        case Some(Right(resolver)) => Seq(resolver.typescheme.raw.toString)
        case None =>
          proto.getOp(Name(member, Span.Null)) match {
            // FIXME: Render ops more better
            case Some(m) => displayMethod(m)
            case None    => Nil
          }
      }
    }

    if (sig.isEmpty) {
      fail(s"No field `$member` found on `${proto.selfType.displayString}`")
    }

    sig
  }

  def page(elems: Seq[Value], after: Seq[Value] = Seq.empty): Value = {
    val struct = Value.Struct(
      "data" -> Value.Array(elems: _*)
    )
    if (after.nonEmpty) {
      struct ++ Value.Struct("after" -> Value.Array(after: _*))
    } else {
      struct
    }
  }
}
