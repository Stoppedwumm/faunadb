package fauna.ast

import fauna.lang.syntax._
import fauna.model.runtime.Effect
import fauna.model.EventSet
import fauna.repo.query.Query
import fauna.repo.store.AbstractIndex
import fauna.storage.Event

abstract class AbstractFoldFunction[A] extends QFunction {
  val effect = Effect.Read

  def functionName: String

  def collectionName: String

  protected def toLiteral(value: A, scanned: Long, pos: Position): Query[R[Literal]]

  protected def accumulate(
    initial: A,
    elems: Iterable[Literal],
    pos: Position): Query[R[A]]

  protected def fold(
    seed: A,
    collection: Literal,
    ec: EvalContext,
    pos: Position): Query[R[Literal]] = {
    Casts.Iterable(collection, pos at collectionName) match {
      case Right(ArrayL(elems)) =>
        accumulate(seed, elems, pos) flatMapT { value =>
          toLiteral(value, elems.size, pos)
        }

      case Right(PageL(elems, unmapped, before, after)) =>
        accumulate(seed, elems, pos) flatMapT { value =>
          toLiteral(value, elems.size, pos) mapT { lit =>
            PageL(List(lit), unmapped, before, after)
          }
        }

      case Right(SetL(set)) =>
        foldSet(seed, set, ec, pos) flatMapT { case (value, scanned) =>
          toLiteral(value, scanned, pos)
        }

      case l @ Left(_) =>
        Query.value(l)
    }
  }

  private def foldSet(
    seed: A,
    set: EventSet,
    ec: EvalContext,
    pos: Position): Query[R[(A, Long)]] =
    Query.withBytesReadDelta {
      ReadAdaptor.setForRead(ec.auth, set, pos at functionName) flatMapT { set =>
        val from = Event.MinValue
        val to = Event.MaxValue

        val pageQ = Query.repo flatMap { repo =>
          // workaround: Set the page size so that result pages
          // will not blow the max query width. Partitioned indexes will
          // over-fetch, so reduce the size by the over-fetch factor.
          val pageSize = math.min(
            ReadAdaptor.MaxPageSize,
            math
              .floor(repo.queryMaxWidth.toDouble / AbstractIndex.OverFetchFactor)
              .toInt)
          if (set.shape.isHistorical) {
            set.history(ec, from, to, pageSize, true)
          } else {
            set.snapshot(ec, from, to, pageSize, true)
          }
        }

        val filteredQ = if (set.isFiltered) {
          pageQ.selectMT { elem =>
            ec.auth.checkReadPermission(
              elem.value.scopeID,
              elem.value.docID
            )
          }
        } else {
          pageQ
        }

        // an option is used to control when to call strategy.initialValue()
        val seedR: R[(Option[A], Long)] = Right((Option.empty[A], 0L))

        filteredQ.foldLeftMT(seedR) {
          case (Right((acc, scanned)), elems) =>
            val values = elems.map { elem =>
              val event = elem.value

              set.getLiteral(event)
            }

            val initial = acc getOrElse seed

            accumulate(initial, values, pos) mapT { value =>
              (Some(value), scanned + values.size)
            }

          case (errs @ Left(_), _) =>
            Query.value(errs)
        }
      }
    } flatMap {
      case (Right((Some(values), scanned)), Some(delta)) =>
        val setOps = (scanned.toDouble / ReadAdaptor.MaxPageSize).ceil
        Query.addSets(setOps.toInt, set.count, delta) map { _ =>
          Right((values, scanned))
        }

      case (Right((Some(values), scanned)), None) =>
        Query.value(Right((values, scanned)))

      case (Right((None, _)), _) =>
        // This should never happen because foldLeftMT will call the closure even
        // if the page is empty, so initial value should be assigned.
        Query.value(Left(List.empty))
      case (Left(errs), _) =>
        Query.value(Left(errs))
    }
}
