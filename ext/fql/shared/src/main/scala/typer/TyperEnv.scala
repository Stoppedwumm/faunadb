package fql.typer

import fql.ast.{ Expr, TypeExpr }
import fql.ast.display._
import fql.ast.Name
import fql.ast.Span
import fql.env.TypeEnv
import fql.error.TypeError
import fql.Result
import scala.collection.{ Map => IMap, Set => ISet }
import scala.collection.immutable.{ ArraySeq, SortedMap }
import scala.collection.mutable.{
  ArrayBuffer,
  Map => MMap,
  SeqMap => MSeqMap,
  Set => MSet,
  SortedMap => MSortedMap
}
import scala.concurrent.duration._

sealed trait EnvCheckResult {
  def toOption: Option[TypeExpr.Scheme]
  def get: TypeExpr.Scheme
}

class EnvCheckThunk {
  private var res = Option.empty[TypeExpr.Scheme]
  def result: EnvCheckResult = new EnvCheckResult {
    def toOption = res
    def get = res.get
  }
  def set(r: TypeExpr.Scheme): Unit = {
    require(res.isEmpty)
    res = Some(r)
  }
}

trait TyperEnvCompanion {
  final class EnvBuilder {
    private[typer] val typeDefs =
      MSeqMap.empty[String, Either[TypeShape, (TypeExpr, EnvCheckThunk)]]
    private[typer] val valueDefs =
      MSeqMap.empty[String, (Option[TypeExpr], Option[Expr], EnvCheckThunk)]
    private[typer] val typeChecks =
      ArrayBuffer.empty[(Type, Expr, EnvCheckThunk)]

    def addTypeShape(name: String, ty: TypeShape) = {
      typeDefs += name -> Left(ty)
    }
    def addTypeDef(name: String, defn: TypeExpr): EnvCheckResult = {
      require(!typeDefs.contains(name), s"typeDef $name already defined")
      val thunk = new EnvCheckThunk
      typeDefs += name -> Right((defn, thunk))
      thunk.result
    }

    def hasGlobal(name: String) = valueDefs.contains(name)

    def addGlobal(
      name: String,
      sig: Option[TypeExpr],
      defn: Option[Expr]): EnvCheckResult = {
      require(sig.orElse(defn).nonEmpty, "either signature or defn must be defined")
      require(!valueDefs.contains(name), s"valueDef $name already defined")
      val thunk = new EnvCheckThunk
      valueDefs += (name -> (sig, defn, thunk))
      thunk.result
    }

    def addTypeCheck(sig: Type, defn: Expr): EnvCheckResult = {
      val thunk = new EnvCheckThunk
      typeChecks += ((sig, defn, thunk))
      thunk.result
    }
  }

  def newEnvBuilder = new EnvBuilder

  case class DepInfo(
    depOrder: Iterable[String],
    // definitions which are detected as recursive. Within a co-recursive cycle,
    // only first definition in lexical name order will be included.
    recursive: ISet[String],
    varDeps: IMap[String, ISet[String]],
    tyDeps: IMap[String, ISet[String]]
  )

  object DepInfo {
    def apply(defns: SortedMap[String, Expr]): DepInfo = {
      val varDeps = MSortedMap.empty[String, MSet[String]]
      val tyDeps = MMap.empty[String, MSet[String]]

      defns.foreach { case (n, e) =>
        e.foreachName {
          case Left(t)  => tyDeps.getOrElseUpdate(n, MSet.empty) += t.str
          case Right(v) => varDeps.getOrElseUpdate(n, MSet.empty) += v.str
        }
      }

      val recursive = MSet.empty[String]
      val depOrder = MSeqMap.empty[String, Unit]

      def go(name: String, pending: Set[String]): Unit = {
        if (!defns.contains(name)) return ()

        varDeps.getOrElse(name, Nil).foreach { dep =>
          if (pending.contains(dep)) {
            recursive += dep
          } else {
            go(dep, pending + dep)
          }
        }
        depOrder += (name -> ())
      }

      defns.keys.foreach { n =>
        if (!depOrder.contains(n)) go(n, Set(n))
      }

      DepInfo(depOrder.keys, recursive, varDeps, tyDeps)
    }
  }
}

trait TyperEnv { self: Typer =>

  def typeEnv(
    builder: Typer.EnvBuilder,
    deadline: Deadline = Int.MaxValue.seconds.fromNow): Result[TypeEnv] = {
    val (typer0, tfails) = this.typeTDefs(builder)
    val Typecheck(typer1, vfails) = typer0.typeValDefs(builder)(deadline)

    val env = TypeEnv(typer1.globals, typer1.typeShapes)
    val fails = tfails ++ vfails

    if (fails.nonEmpty) {
      Result.Err(fails)
    } else {
      Result.Ok(env)
    }
  }

  private def buildArityShapes(builder: Typer.EnvBuilder) = {
    val tyNames = (typeShapes.keys ++ builder.typeDefs.keys).toSet

    // generate shapes w/ correct arities. we walk type names directly to
    // recover params in the correct order
    builder.typeDefs.map {
      case (n, Left(shape)) => n -> shape

      case (n, Right((tpe, _))) =>
        val free = MSeqMap.empty[String, Unit]
        tpe.foreachTypeName((n => free.getOrElseUpdate(n.str, ())), tyNames)
        val self = Type.Named(
          Name(n, tpe.span),
          tpe.span,
          free.keys
            .map(n => Type.Skolem(Name(n, tpe.span), Type.Level.Zero))
            .to(ArraySeq))
        n -> TypeShape(self = self.typescheme)
    }.toMap
  }

  // re-expose typeTExpr0 but with some defaults
  private[TyperEnv] def checkTExpr0(tpe: TypeExpr) =
    typeTExpr0(
      tpe,
      checked = true,
      allowGenerics = true,
      allowVariables = true
    )(Map.empty, rootAlt, Type.Level.Zero, pol = true)

  private[TyperEnv] def typeTDefs(builder: Typer.EnvBuilder) = {
    val fails = List.newBuilder[TypeError]
    val shapes = Map.newBuilder[String, TypeShape]
    val typer = this.withShapes(buildArityShapes(builder))

    builder.typeDefs.foreach {
      case (n, Left(shape)) =>
        shapes += (n -> shape)

      case (n, Right((tpe, thnk))) =>
        val (ty, es) = typer.checkTExpr0(tpe)

        // if the check failed, return an Any with the correct params
        val ty0 = if (es.nonEmpty) Type.Any(tpe.span) else ty

        val ts = typer.typeShapes(n)
        val self = ts.self
        val params = ts.tparams
        val sch = ty0.typescheme
        val expr = typer.toTypeSchemeExpr(sch).copy(params = params)
        val shape = TypeShape(
          self = self,
          alias = Option.when(es.nonEmpty)(sch),
          isPersistable = ts.isPersistable,
          docType = ts.docType
        )

        thnk.set(expr)
        shapes += (n -> shape)
        fails ++= es
    }

    (typer.withShapes(shapes.result()), fails.result())
  }

  private[TyperEnv] def typeValDefs(builder: Typer.EnvBuilder)(
    implicit deadline: Deadline) = {
    val fails = List.newBuilder[TypeError]
    val globalsB = Map.newBuilder[String, TypeScheme]
    val toInfer = Map.newBuilder[String, Expr]
    val toCheck = ArrayBuffer.newBuilder[(Type, Expr)]

    builder.valueDefs.foreach { case (n, (sig, defn, _)) =>
      sig match {
        case Some(tpe) =>
          val (ty, es) = checkTExpr0(tpe)
          val sch = ty.typescheme
          fails ++= es
          globalsB += (n -> sch)
          defn.foreach { e => toCheck += (ty -> e) }
        case None =>
          toInfer += (n -> defn.get)
      }
    }

    builder.typeChecks.foreach { case (ty, e, _) =>
      toCheck += (ty -> e)
    }

    val globals = globalsB.result()
    val typer = this.withGlobals(globals)

    val Typecheck(inferred, es) = typer.typeRecEnv(toInfer.result())
    fails ++= es

    val typer2 = typer.withGlobals(inferred)

    toCheck.result().foreach { case (sig, e) =>
      val Typecheck(_, es) = typer2.typeAnnotatedExpr(e, sig)
      fails ++= es
    }

    builder.valueDefs.foreach {
      case (n, (None, _, thnk)) =>
        // re-anneal inferred types at L0
        val TypeScheme.Polymorphic(raw, _) = inferred(n)
        val ann = annealValue(raw, rootAlt, Type.Level.Zero)
        thnk.set(typer2.toTypeSchemeExpr(TypeScheme.Simple(ann)))
      case (n, (Some(_), _, thnk)) =>
        thnk.set(typer2.toTypeSchemeExpr(globals(n)))
      case _ =>
    }
    builder.typeChecks.foreach { case (t, _, thnk) =>
      val annealed = typer2.annealValue(t, rootAlt, Type.Level.Zero, Set.empty)
      thnk.set(typer2.toTypeSchemeExpr(TypeScheme.Simple(annealed)))
    }

    Typecheck(typer2, fails.result())
  }

  // We can't typecheck the whole env in one pass, because
  // freshen() breaks the bidirectional dependency link between variables
  // (which is mostly what we want when one is polymorphic). We get around
  // this by building a dependency graph with which we order typechecking.
  //
  // TODO: If we could figure out how to improve freshen(), we could drop
  // the complexity below.
  private[TyperEnv] def typeRecEnv(defns: Map[String, Expr])(
    implicit deadline: Deadline): Typecheck[Map[String, TypeScheme]] =
    dbgFrame("Typing global env:") {
      val info = Typer.DepInfo(defns.to(SortedMap))
      val (rec, nonRec) = info.depOrder.partition(info.recursive.contains)

      val LvlZero = Type.Level.Zero
      val LvlOne = Type.Level.Zero.incr

      val vars = defns.keys.map(_ -> freshVar(Span.Null)(LvlOne)).toMap
      val vctx =
        vars.view.mapValues(TypeScheme.Polymorphic(_, LvlZero)).toMap

      if (dbgEnabled) {
        if (rec.nonEmpty) dbg(s"Recursive: ${rec.mkString(", ")}")
        info.depOrder.foreach(n => dbg(s"$n: ${defns(n).display}"))
      }

      // set recursive functions to Any
      // TODO: figure out global inference with co-recursion
      rec.foreach { n =>
        val e = defns(n)
        val tc = constrain(Type.Any(Span.Null), vars(n), e.span)(
          Map.empty,
          rootAlt,
          LvlZero)
        assert(!tc.isFailed)
      }

      for {
        // reject recursive defns which reach here (i.e. not explicitly typed)
        _ <- Typecheck.lift(
          rec.map(n => TypeError.RecursiveVariable(defns(n).span, n)))

        // type non-recursive in context
        _ <- Typecheck.sequence(nonRec.map { n =>
          val e = defns(n)
          typeExpr0(e)(vctx, rootAlt, LvlOne, deadline).flatMap { rhs =>
            constrain(rhs, vars(n), e.span)(Map.empty, rootAlt, LvlZero)
          }
        })

        free = freeAlts(vctx)(rootAlt)

        ret = defns.keys.view.map { n =>
          val raw = vars(n)
          val ann = annealValue(raw, rootAlt, LvlOne, free)
          val sch = TypeScheme.Polymorphic(ann, LvlZero)
          n -> sch
        }.toMap

        _ <- Typecheck.sequence(ret.values.view.map { sch =>
          Typecheck.lift {
            findProjections(sch.raw).map(p => TypeError.InvalidProjection(p.span))
          }
        })
      } yield {

        if (dbgEnabled) {
          dbg("Typed as:")
          info.depOrder.foreach { n =>
            dbg(s"  $n: ${toTypeSchemeExpr(ret(n)).display}")
          }
        }

        ret
      }
    }
}
