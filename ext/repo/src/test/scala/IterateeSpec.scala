package fauna.repo.test

import fauna.repo._
import fauna.repo.query.Query

class IterateeSpec extends Spec {

  implicit val ctx = CassandraHelper.context("repo", Seq.empty)

  abstract class IntIntIteratee
      extends RangeIteratee[Int, Int] {

    def apply(row: Int): Query[Option[ColIteratee[Int]]] =
      Query.some(ColIteratee { cols =>
        colIter(row, cols)
      })

    def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]]
  }

  val input = Seq((1, 1), (1, 2), (2, 1), (2, 2), (2, 3), (3, 1))

  "RangeIteratee" - {
    "processes all input" in {
      var seen = Seq.empty[(Int, Int)]

      val iter = new IntIntIteratee {
        def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
          cols match {
            case Some(cols) =>
              seen ++= cols map { (row, _) }
              Query.some(ColIteratee {
                colIter(row, _)
              })
            case None => Query.none
          }
        }

      val pq = PagedQuery(input)
      ctx ! iter.run(pq)

      seen should equal (input)
    }

    "processes each row" in {
      var seen = Map.empty[Int, Iterable[Int]]

      val iter = new IntIntIteratee {
        def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
          cols match {
            case Some(cols) =>
              seen += row -> cols
              Query.some(ColIteratee {
                colIter(row, _)
              })
            case None => Query.none
          }
        }

      val pq = PagedQuery(input)
      ctx ! iter.run(pq)

      val expected = input.foldLeft(Map.empty[Int, Seq[Int]]) {
        case (acc, (k, v)) =>
          if (acc.contains(k)) {
            acc + (k -> (acc(k) :+ v))
          } else {
            acc + (k -> Seq(v))
          }
      }

      seen should equal (expected)
    }

    "skips rows with SkipRowException" in {
      for (badRow <- 1 to 3) {
        var seen = Map.empty[Int, Iterable[Int]]

        val iter = new IntIntIteratee {
          override val isSkippingIteratee = true

          def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
            if (row == badRow) {
              Query.fail(RangeIteratee.SkipRowException)
            } else {
              cols match {
                case Some(cols) =>
                  seen += row -> cols
                  Query.some(ColIteratee {
                    colIter(row, _)
                  })
                case None => Query.none
              }
            }
        }

        val pq = PagedQuery(input)
        ctx ! iter.run(pq)

        val expected = input.foldLeft(Map.empty[Int, Seq[Int]]) {
          case (acc, (k, v)) =>
            if (k == badRow) {
              acc
            } else if (acc.contains(k)) {
              acc + (k -> (acc(k) :+ v))
            } else {
              acc + (k -> Seq(v))
            }
        }
        seen should equal (expected)
      }
    }

    "augments the input with finish" in {
      var seen = Seq.empty[(Int, Int)]

      val iter = new IntIntIteratee {
        private[this] var finished = false

        def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
          cols match {
            case Some(cols) =>
              seen ++= cols map { (row, _) }
              Query.some(ColIteratee {
                colIter(row, _)
              })
            case None => Query.none
          }

        override def finish() =
          if (!finished) {
            finished = true
            Query.some(PagedQuery(Seq((4, 1))))
          } else {
            Query.none
          }
      }

      val pq = PagedQuery(input)
      ctx ! iter.run(pq)

      seen should equal (input :+ ((4, 1)))

    }
  }

  "throws an illegal state exception when skipping a row after the end" in {
    val iter = new IntIntIteratee {
      override val isSkippingIteratee = true

      def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
        cols match {
          case Some(_) => Query.some(ColIteratee { colIter(row, _) })
          case None => throw RangeIteratee.SkipRowException
        }
    }

    val pq = PagedQuery(input)
    assertThrows[IllegalStateException] {
      (ctx ! iter.run(pq))
    }
  }

  "throws an illegal state exception when skipping a row and the iteratee doesn't support skipping" in {
    val iter = new IntIntIteratee {
      def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
        cols match {
          case Some(_) => Query.some(ColIteratee { colIter(row, _) })
          case None => throw RangeIteratee.SkipRowException
        }
    }

    val pq = PagedQuery(input)
    assertThrows[IllegalStateException] {
      (ctx ! iter.run(pq))
    }
  }

  "ColIteratee" - {
    "halts processing" in {
      var seen = Seq.empty[(Int, Int)]

      val iter = new IntIntIteratee {
        def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
          cols match {
            case Some(cols) =>
              if (row < 3) {
                seen ++= cols map { (row, _) }
                Query.some(ColIteratee {
                  colIter(row, _)
                })
              } else {
                // halt after row 2
                Query.none
              }
            case None => Query.none
          }
      }

      val pq = PagedQuery(input)
      ctx ! iter.run(pq)

      seen should equal (input filter { case (k, _) => k < 3 })
    }

    "skips rows after transaction limits are reached" in {
      var seen = Seq.empty[(Int, Int)]

      val iter = new IntIntIteratee {
        override val isSkippingIteratee = true

        def colIter(row: Int, cols: Option[Iterable[Int]]): Query[Option[ColIteratee[Int]]] =
          cols match {
            case Some(cols) =>
              if (row < 3) {
                seen ++= cols map { (row, _) }
                Query.some(ColIteratee {
                  colIter(row, _)
                })
              } else {
                // throw after row 2
                Query.fail(TxnTooLargeException(0, 0, "bytes"))
              }
            case None => Query.none
          }
      }

      val pq = PagedQuery(input)
      ctx ! iter.run(pq)

      seen should equal (input filter { case (k, _) => k < 3 })
    }
  }
}
