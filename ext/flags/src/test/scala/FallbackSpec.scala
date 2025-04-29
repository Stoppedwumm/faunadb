package fauna.flags.test

import fauna.atoms._
import fauna.flags._
import fauna.prop.Prop

class FallbackSpec extends Spec {
  once("get") {
    for {
      id <- Prop.long
    } {
      val svc1 = (new Service.Null).withFallbacks(account = Map(RunQueries -> false))
      val svc2 = svc1.withFallbacks(account = Map(RunTasks -> false))
      val props = AccountProperties(AccountID(id), Map.empty)

      val res1: Option[AccountFlags] = svc1
        .getUncached[AccountID, AccountProperties, AccountFlags](props)
        .value
        .get
        .get
      res1 shouldEqual Some(
        AccountFlags(AccountID(id), Map(RunQueries.key -> false)))

      val res2: Option[AccountFlags] = svc2
        .getUncached[AccountID, AccountProperties, AccountFlags](props)
        .value
        .get
        .get
      res2 shouldEqual Some(
        AccountFlags(
          AccountID(id),
          Map(RunQueries.key -> false, RunTasks.key -> false)))
    }
  }
}
