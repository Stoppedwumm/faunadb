package fauna.repo.test

import fauna.repo.service.BarService

class BarServiceSpec extends Spec {

  "valid backup names" in {
    BarService.isValidBackupName("b_1-2") shouldEqual true
    BarService.isValidBackupName("backup1234567890") shouldEqual true
    BarService.isValidBackupName("ABCDEFGHIJKLMNOPQRSTUVWXYZ") shouldEqual true
    BarService.isValidBackupName("abcdefghijklmnopqrstuvwxyz") shouldEqual true
  }

  "Check backup names with puncutation" in {
    BarService.isValidBackupName("backup*1") shouldEqual false
    BarService.isValidBackupName("""backup\2""") shouldEqual false
    BarService.isValidBackupName("backup/3") shouldEqual false
    BarService.isValidBackupName("backup.4") shouldEqual false
    BarService.isValidBackupName("backup[5") shouldEqual false
    BarService.isValidBackupName("backup]6") shouldEqual false
    BarService.isValidBackupName("backup(7") shouldEqual false
    BarService.isValidBackupName("backup)8") shouldEqual false
  }

}
