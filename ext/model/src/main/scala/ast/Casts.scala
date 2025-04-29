package fauna.ast

import fauna.atoms._
import fauna.lang.{ MidstUnit => MUnit }
import fauna.model.{ DocumentsSet, EventSet, IndexSet, SchemaSet }
import fauna.storage._
import fauna.storage.doc.{ Diff => IDiff }
import fauna.storage.index.IndexTerm
import java.util.regex.PatternSyntaxException

sealed abstract class TypeCast[+A] extends ((Literal, => Position) => R[A]) {
  def apply(r: Literal, pos: => Position): R[A]

  def opt(r: Option[Literal], pos: => Position): R[Option[A]] =
    r match {
      case Some(r) => apply(r, pos).map (Some(_))
      case None    => Right(None)
    }

  def orElse[A1 >: A](r: Option[Literal], alt: => A1, pos: => Position): R[A1] =
    r map { apply(_, pos) } getOrElse Right(alt)
}

object Casts {
  private[this] def TypeCast[A](ts: Type*)(pf: PartialFunction[Literal, A]): TypeCast[A] =
    new TypeCast[A] {
      def apply(r: Literal, pos: => Position): R[A] =
        (pf andThen (Right(_))).applyOrElse(r, { _: Literal => r match {
          case UnresolvedRefL(original)
            if (ts.exists(Type.isSub(Type.Identifier, _))) =>
              Left(List(UnresolvedRefError(original, pos)))
          case _                                          =>
            Left(List(InvalidArgument(ts.toList, r.rtype, pos)))
        }})
    }

  val Any = TypeCast(Type.Any) { case r => r }
  val Scalar = TypeCast(Type.Scalar) { case r: ScalarL => r }
  val Integer = TypeCast(Type.Integer) { case LongL(l) => l }
  val Number = TypeCast(Type.Number) { case DoubleL(d) => Left(d); case LongL(l) => Right(l) }
  val Boolean = TypeCast(Type.Boolean) { case BoolL(b) => b }
  val String = TypeCast(Type.String) { case StringL(s) => s }
  val Null = TypeCast(Type.Null) { case NullL => () }

  val Lambda = TypeCast(Type.Lambda) { case l: LambdaL => l }
  val Object = TypeCast(Type.Object) { case o: ObjectL => o }
  val Array = TypeCast(Type.Array) { case a: ArrayL => a }

  val Ref = TypeCast(Type.Ref) { case ref: RefL => ref }
  val ClassRef = TypeCast(Type.CollectionRef) { case ref @ RefL(_, CollectionID(_)) => ref }
  val IndexRef = TypeCast(Type.IndexRef) { case ref @ RefL(_, IndexID(_)) => ref }
  val DatabaseRef = TypeCast(Type.DatabaseRef) { case ref @ RefL(_, DatabaseID(_)) => ref }
  val KeyRef = TypeCast(Type.KeyRef) { case ref @ RefL(_, KeyID(_)) => ref }
  val UserFunctionRef = TypeCast(Type.UserFunctionRef) { case ref @ RefL(_, UserFunctionID(_)) => ref }

  val StringOrRef = TypeCast(Type.Identifier) {
    case name @ StringL(_) => Left(name)
    case ref @ RefL(_, _)  => Right(ref)
  }

  val StringOrIndexRef = TypeCast(Type.Identifier) {
    case name @ StringL(_) => Left(name)
    case ref @ RefL(_, IndexID(_)) => Right(ref)
  }

  val StringOrUserFunctionRef = TypeCast(Type.Identifier) {
    case name @ StringL(_) => Left(name)
    case ref @ RefL(_, UserFunctionID(_)) => Right(ref)
  }

  val Identifier: TypeCast[IdentifierL] = TypeCast(Type.Ref, Type.Set) {
    case set @ SetL(_)                                      => set
    case RefL(scope, CollectionID(DatabaseID.collID))       => SetL(SchemaSet.Databases(scope))
    case RefL(scope, CollectionID(KeyID.collID))            => SetL(SchemaSet.Keys(scope))
    case RefL(scope, CollectionID(TokenID.collID))          => SetL(SchemaSet.Tokens(scope))
    case RefL(scope, CollectionID(CredentialsID.collID))    => SetL(SchemaSet.Credentials(scope))
    case RefL(scope, CollectionID(CollectionID.collID))     => SetL(SchemaSet.Collections(scope))
    case RefL(scope, CollectionID(IndexID.collID))          => SetL(SchemaSet.Indexes(scope))
    case RefL(scope, CollectionID(TaskID.collID))           => SetL(SchemaSet.Tasks(scope))
    case RefL(scope, CollectionID(UserFunctionID.collID))   => SetL(SchemaSet.UserFunctions(scope))
    case RefL(scope, CollectionID(RoleID.collID))           => SetL(SchemaSet.Roles(scope))
    case RefL(scope, CollectionID(AccessProviderID.collID)) => SetL(SchemaSet.AccessProviders(scope))
    case ref @ RefL(_, _)                                   => ref
  }

  val Unresolved: TypeCast[UnresolvedIdentifierL] = TypeCast(Type.Ref, Type.Set) {
    case id: UnresolvedIdentifierL => id
  }

  // Array, Page, or Set
  val Iterable: TypeCast[IterableL] = new TypeCast[IterableL] {
    def apply(r: Literal, pos: => Position) =
      r match {
        case a @ ArrayL(_)         => Right(a)
        case p @ PageL(_, _, _, _) => Right(p)
        case _ =>
          Identifier(r, pos) match {
            case Right(s @ SetL(_)) => Right(s)
            case Right(_) =>
              Left(List(InvalidArgument(List(Type.AbstractIterable), r.rtype, pos)))
            case Left(List(InvalidArgument(_, _, _))) =>
              Left(List(InvalidArgument(List(Type.AbstractIterable), r.rtype, pos)))
            case Left(err) => Left(err)
          }
      }
  }

  val Set: TypeCast[EventSet] = new TypeCast[EventSet] {
    def apply(r: Literal, pos: => Position) =
      Identifier(r, pos) match {
        case Right(SetL(set)) => Right(set)
        case Right(_)         => Left(List(InvalidArgument(List(Type.Set), r.rtype, pos)))
        case Left(err)        => Left(err)
      }
  }

  val SetOrArray: TypeCast[IterableL] = new TypeCast[IterableL] {
    def apply(r: Literal, pos: => Position): R[IterableL] =
      Iterable(r, pos) match {
        case Right(array: ArrayL) => Right(array)
        case Right(set: SetL)     => Right(set)
        case Right(_: PageL)      => Left(List(InvalidArgument(List(Type.Set, Type.Array), Type.Page, pos)))
        case Left(errs)           => Left(errs)
      }
  }

  val DocAction: TypeCast[DocAction] = new TypeCast[DocAction] {
    def apply(r: Literal, pos: => Position) =
      r match {
        case StringL("create") => Right(Create)
        case StringL("update") => Right(Update)
        case StringL("delete") => Right(Delete)
        case ActionL(Create)   => Right(Create)
        case ActionL(Update)   => Right(Update)
        case ActionL(Delete)   => Right(Delete)
        case r                 => Left(List(InvalidDocAction(r.rtype, pos)))
      }
  }

  val SetAction: TypeCast[SetAction] = new TypeCast[SetAction] {
    def apply(r: Literal, pos: => Position) =
      r match {
        case StringL("add")           => Right(Add)
        case StringL("remove")        => Right(Remove)
        case StringL("create")        => Right(Add)
        case StringL("delete")        => Right(Remove)
        case ActionL(Add | Create)    => Right(Add)
        case ActionL(Remove | Delete) => Right(Remove)
        case r                        => Left(List(InvalidSetAction(r.rtype, pos)))
      }
  }

  val Streamable: TypeCast[StreamableL] = new TypeCast[StreamableL] {
    def apply(r: Literal, pos: => Position): R[StreamableL] =
      r match {
        case l @ SetL(_: IndexSet)                                     => Right(l)
        case l @ SetL(d: DocumentsSet) if d.set.isInstanceOf[IndexSet] => Right(l)

        case l @ RefL(_, DocID(_, UserCollectionID(_))) => Right(l)
        case l @ VersionL(version, _) =>
          version.docID.collID match {
            case UserCollectionID(_) => Right(l)
            case _                   => Left(List(NonStreamableType(l.rtype, pos)))
          }
        case UnresolvedRefL(ref) => Left(List(UnresolvedRefError(ref, pos)))
        case other               => Left(List(NonStreamableType(other.rtype, pos)))
      }
    }

  // Splat equivs

  def ZeroOrMore[T](cast: TypeCast[T]): TypeCast[List[T]] =
    new TypeCast[List[T]] {

      def apply(r: Literal, pos: => Position) =
        Parser.sequence(
          FunctionHelpers
            .toElemsIter(r, pos)
            .map { case (e, pos) => cast(e, pos) } toList)
    }

  def OneOrMore[T](cast: TypeCast[T]): TypeCast[List[T]] =
    new TypeCast[List[T]] {
      private[this] val zero = ZeroOrMore(cast)

      def apply(r: Literal, pos: => Position) =
        zero(r, pos).flatMap {
          case Nil => Left(List(EmptyArrayArgument(pos)))
          case es  => Right(es)
        }
    }

  // Outlier stuff

  // Normally, we would want to use an Option[Foo] instead of Union(Foo, null).  However!  Certain forms such as
  // NativeClassConstructFun use a Nullable instead of an Option[DatabaseRef] because those forms are
  // unary and JSON semantics mandate objects have at least one KV pair.
  def Nullable[T >: Literal](tc: TypeCast[T]): TypeCast[Option[T]] = new TypeCast[Option[T]] {
    def apply(r: Literal, pos: => Position) = {
      r match {
        case NullL => Right(None)
        case _ => tc.opt(Some(r), pos) match {
          // If the nested cast failed because the client supplied the wrong type, indicate
          // that we can also admit a Null in this position.
          // TODO: How do we ensure that an InvalidArgument that is produced from `tc` is in the
          // same position as us? e.g. `NativeClassConstructFun.apply` can produce an error at "scope" position.
          // We can ignore position but for more complicated, nested expressions, where subexpressions' errors are
          // flattened into Left(errs), we could potentially modify an unrelated argument...
          case r @ Right(_) => r
          case Left(errs)   => Left(errs.map {
            case e: InvalidArgument => e.copy(expected = e.expected :+ Type.Null)
            case e                  => e
          })
        }
      }
    }
  }

  // used to support both UNIX microseconds and @ts as timestamp parameters
  val Timestamp = TypeCast(Type.Time) {
    case TimeL(ts) => ts
    case LongL(l)  => fauna.lang.Timestamp.ofMicros(l)
  }

  val Temporal = TypeCast(Type.Temporal) {
    case TimeL(ts) => Right(ts)
    case DateL(ld) => Left(ld)
  }

  val MidstUnit = TypeCast(Type.MidstUnit) {
    case StringL(MUnit(mu)) => mu
  }

  val TransactionTimestamp = TypeCast(Type.Time) {
    case TransactionTimeL | TransactionTimeMicrosL => TransactionTimeL
    case ts @ TimeL(_)                             => ts
    case LongL(l)                                  => TimeL(fauna.lang.Timestamp.ofMicros(l))
  }

  val Diff: TypeCast[IDiff] = new TypeCast[IDiff] {
    def apply(r: Literal, pos: => Position) =
      r match {
        case r: ObjectL => Right(r.toDiff)
        case provided   => Left(List(InvalidArgument(List(Type.Object), provided.rtype, pos)))
      }
  }

  val Path: TypeCast[List[Either[Long, String]]] =
    new TypeCast[List[Either[Long, String]]] {

      def apply(r: Literal, pos: => Position) =
        Parser.sequence(
          FunctionHelpers
            .toElemsIter(r, pos)
            .map {
              case (StringL(k), _) => Right(Right(k))
              case (LongL(i), _)   => Right(Left(i))
              case (provided, pos) =>
                Left(
                  List(
                    InvalidArgument(
                      List(Type.String, Type.Integer),
                      provided.rtype,
                      pos)))
            } toList)
    }

  val Normalizer: TypeCast[NormalizerType] = new TypeCast[NormalizerType] {

    def apply(r: Literal, pos: => Position) =
      r match {
        case StringL("NFKCCaseFold") => Right(NFKCCaseFold)
        case StringL("NFC")          => Right(NFC)
        case StringL("NFD")          => Right(NFD)
        case StringL("NFKC")         => Right(NFKC)
        case StringL("NFKD")         => Right(NFKD)
        case r                       => Left(List(InvalidNormalizer(r.rtype, pos)))
      }
  }

  val MatchTerm = new TypeCast[IndexTerm] {
    def apply(r: Literal, pos: => Position) =
      try {
        Right(Literal.toIndexTerm(r))
      } catch {
        case UnresolvedRefL.IRException(_) =>
          Left(List(InvalidArgument(List(Type.Any), r.rtype, pos)))
      }
  }

  val Regex = new TypeCast[scala.util.matching.Regex] {
    def apply(l: Literal, pos: => Position) =
      l match {
        case StringL(str) =>
          try {
            Right(str.r)
          } catch {
            case _: PatternSyntaxException =>
              Left(List(InvalidRegex(str, pos)))
          }
        case l => Left(List(InvalidArgument(List(Type.String), l.rtype, pos)))
      }
  }

  val Field = new TypeCast[(String, Literal)] {
    def apply(r: Literal, pos: => Position): R[(String, Literal)] =
      r match {
        case ArrayL(List(StringL(name), lit)) => Right((name, lit))
        case _                                => Left(List(InvalidArgument(List(Type.Field), r.rtype, pos)))
      }
  }
}
