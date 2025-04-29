package fauna.repo.test

import fauna.atoms._
import fauna.cluster.topology.{ ReplicaTopology, SegmentOwnership }
import fauna.repo.cassandra.CassandraTopologiesInitializer

class CassandraTopologiesInitializerSpec extends Spec {
  "whatever" in {
    val testData =
      List(
        (-8861112963042326642L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (-8334129330001026116L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (-7826267074326988042L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (-7805903748342391076L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (-7545412439081832999L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (-7486979693720453943L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (-6634562458217042630L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (-6572819867513487851L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (-6481198460869453906L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (-6162355769266782220L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (-6122928005255016431L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (-6094594175089033512L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (-6093761031467174836L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (-5996602625273428405L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (-5899321064584533774L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (-5599645743574378924L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (-5597419271126955808L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (-5572072910814350392L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-5513057071751911444L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (-5331192282041059991L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (-5127011451756335638L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-5118567199828758323L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (-4720021388634086087L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-4571197235539157811L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-4033548109480449440L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (-3916628798405160118L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (-3469815823333559827L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (-3345369155586533938L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-3214123380017512245L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (-1877545263518988179L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (-1370569214525123061L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (-989951222935539404L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (266428359727440202L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (560177242251175728L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (593229591312934188L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (763101362955146216L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (1279964631469799899L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (1283071440871918194L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (1321030228777752789L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (1345014721000820101L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (1782653526489071988L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (1924170921664910566L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (2191080635552266047L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (2244902339446282680L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (2530989651112625641L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (2687972571356470572L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (3226163322981959029L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (3551045153293369183L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (3671369324599820379L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (3926121325474110557L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (3970904346747077549L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (4103987049511082931L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"),
        (4265451382042388383L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (4359991218021955780L,"us-central-c","58fba679-b451-4dc2-a698-7abb5940dd94"),
        (4436869824840619231L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (4531479294320410611L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (4934499525702758834L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (5084896875496930759L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (5339187007859650975L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (5393128457007310793L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (5541795191220443889L,"us-central-c","8e22ff83-be07-49d8-9cb0-db976c6a7a2c"),
        (5678958641404134508L,"us-central-a","4689a38d-7c4c-4028-9348-7dae3431e6bf"),
        (5799826761224228404L,"us-central-c","2494db80-98bf-4c50-81d7-e0042479ef87"),
        (5800304196146440682L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (6064402532556318617L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (6678282850978110471L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (7852863627765542739L,"us-central-b","0adf1f14-47b3-4c4b-b980-0b0f9ee1a118"),
        (8008086431258218321L,"us-central-b","7b1937a5-e32f-482d-b3c7-2777a192f72f"),
        (8044646551203825822L,"us-central-b","91d681a0-3cb4-4eac-85c3-65709b479e74"),
        (8140522446437770660L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (8400265003354744352L,"us-central-a","83a4286c-56ef-45ed-aa33-8e85f66786a7"),
        (9143473929864013568L,"us-central-a","3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")
      )

    testData.size should equal (9 * 8)

    val dataFn = testData map { case (t, r, h) => (t, (t, HostID(h), r)) } toMap
    val topology = CassandraTopologiesInitializer.create(dataFn.keys.toSeq.sorted, dataFn.apply)
    topology.size should equal (3) // 3 replicas
    topology foreach {
      case (_, o) =>
        o.size should equal (3 * 8) // total of 3x8 tokens in a replica
        o.groupBy { _.host } foreach { case (_, t) => t.size should equal (8) } // exactly 8 tokens per host
      }

    val expectedTopology =
      Map(
        "us-central-a" -> Vector(
          SegmentOwnership(Location(-7805903748342391076L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(-7486979693720453943L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(-6634562458217042630L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(-6122928005255016431L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(-6093761031467174836L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(-5599645743574378924L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(-5513057071751911444L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(-5118567199828758323L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(-3214123380017512245L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(560177242251175728L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(593229591312934188L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(1279964631469799899L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(1345014721000820101L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(1924170921664910566L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(2530989651112625641L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(2687972571356470572L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(3926121325474110557L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(4103987049511082931L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(4265451382042388383L),HostID("4689a38d-7c4c-4028-9348-7dae3431e6bf")),
          SegmentOwnership(Location(5678958641404134508L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(6064402532556318617L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(8140522446437770660L),HostID("83a4286c-56ef-45ed-aa33-8e85f66786a7")),
          SegmentOwnership(Location(8400265003354744352L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7")),
          SegmentOwnership(Location(9143473929864013568L),HostID("3e5c3d02-1e0f-4cd2-9167-8e2b00a85eb7"))
        ),
        "us-central-b" -> Vector(
          SegmentOwnership(Location(-8861112963042326642L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(-8334129330001026116L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(-6572819867513487851L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(-6094594175089033512L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(-5996602625273428405L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(-5899321064584533774L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(-5597419271126955808L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(-5331192282041059991L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(-4033548109480449440L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(-3469815823333559827L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(763101362955146216L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(1321030228777752789L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(2191080635552266047L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(2244902339446282680L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(3226163322981959029L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(3551045153293369183L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(3671369324599820379L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(4531479294320410611L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(5084896875496930759L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(5800304196146440682L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(6678282850978110471L),HostID("0adf1f14-47b3-4c4b-b980-0b0f9ee1a118")),
          SegmentOwnership(Location(7852863627765542739L),HostID("7b1937a5-e32f-482d-b3c7-2777a192f72f")),
          SegmentOwnership(Location(8008086431258218321L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74")),
          SegmentOwnership(Location(8044646551203825822L),HostID("91d681a0-3cb4-4eac-85c3-65709b479e74"))
        ),
        "us-central-c" -> Vector(
          SegmentOwnership(Location(-7826267074326988042L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(-7545412439081832999L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(-6481198460869453906L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(-6162355769266782220L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-5572072910814350392L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-5127011451756335638L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-4720021388634086087L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-4571197235539157811L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(-3916628798405160118L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-3345369155586533938L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(-1877545263518988179L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(-1370569214525123061L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(-989951222935539404L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(266428359727440202L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(1283071440871918194L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(1782653526489071988L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(3970904346747077549L),HostID("58fba679-b451-4dc2-a698-7abb5940dd94")),
          SegmentOwnership(Location(4359991218021955780L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(4436869824840619231L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(4934499525702758834L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(5339187007859650975L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(5393128457007310793L),HostID("8e22ff83-be07-49d8-9cb0-db976c6a7a2c")),
          SegmentOwnership(Location(5541795191220443889L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87")),
          SegmentOwnership(Location(5799826761224228404L),HostID("2494db80-98bf-4c50-81d7-e0042479ef87"))
        )
      )

    topology should equal (expectedTopology)

    // For additional sanity, initialize a replica topology from each of the created
    // topologies and confirm they throw no exceptions
    topology foreach { case (_, o) => ReplicaTopology.create(o) }
  }
}
