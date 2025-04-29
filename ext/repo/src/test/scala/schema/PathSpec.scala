package fauna.repo.test

import fauna.repo.schema.Path

class PathSpec extends Spec {
  "toString" in {
    Path().toString shouldBe ""
    Path(Right("a")).toString shouldBe "a"
    Path(Right("a"), Right("b")).toString shouldBe "a.b"
    Path(Right("a"), Right("b"), Right("c")).toString shouldBe "a.b.c"

    Path(Right("a"), Left(3)).toString shouldBe "a[3]"
    Path(Left(2), Left(3)).toString shouldBe "[2][3]"
    Path(Left(2), Right("c")).toString shouldBe "[2].c"
  }
}
