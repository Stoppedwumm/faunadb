package fauna.api.test

import fauna.api.fql1.APIResponse
import fauna.api.AdminApplication
import fauna.repo.data.InitializeResult

class AdminApplicationSpec extends Spec {

  "AdminApplication" - {
    "should convert Initialized to None" in {
      AdminApplication.toResponse(InitializeResult.Initialized) shouldBe empty
    }

    "should convert InvalidReplicaName to JSON" in {
      AdminApplication.toResponse(InitializeResult.InvalidReplicaName) should
        contain(APIResponse.BadRequest("Replica name is invalid."))
    }

    "should convert ReplicaNameAlreadySet to JSON" in {
      val replicaName = "old_replica_name"

      AdminApplication.toResponse(InitializeResult.ReplicaNameAlreadySet(replicaName)) should
        contain(APIResponse.BadRequest(s"Replica name is already set to '$replicaName'."))
    }
  }
}
