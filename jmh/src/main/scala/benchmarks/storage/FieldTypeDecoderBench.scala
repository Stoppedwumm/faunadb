package benchmarks.storage

import benchmarks.storage.FieldTypeDecoderBench._
import fauna.storage.doc.{ Data, Field }
import fauna.storage.ir.{ LongV, MapV }
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput))
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(value = 1)
class FieldTypeDecoderBench {

  @Benchmark
  def decodeDeeplyNestedMapV() = {
    val res = field1.read(data1.fields)
    assert(res == expectedRes)
  }

}

object FieldTypeDecoderBench {

  val field1 = Field[List[(String, Long)]]("segment", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9", "p10")
  val data1 = Data(
    MapV("segment" ->
      MapV("p1" ->
        MapV("p2" ->
          MapV("p3" ->
            MapV("p4" ->
              MapV("p5" ->
                MapV("p6" ->
                  MapV("p7" ->
                    MapV("p8" ->
                      MapV("p9" ->
                        MapV("p10" ->
                          MapV(
                            "foo" -> LongV(1),
                            "bar" -> LongV(2))))))))))))))
  val expectedRes = Right(List("foo" -> 1, "bar" -> 2))


}
