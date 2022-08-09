import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedINDDynamicProgrammingSolver, VersionRange}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

class NewTemporalINDTest extends AnyFlatSpec{

  "Greedy-Non-Optimal-Test" should "compute correct Mapping" in {
    val history1 = toHistory(Map(
      (1,Set("a","b")),
      (3,Set("c","d")),
      (6,Set("e","f"))
    ))
    val history2 = toHistory(Map(
      (1,Set("e","f")),
      (2,Set("c","d")),
      (3,Set("a","b")),
      (4,Set("e","f"))
    ))
    GLOBAL_CONFIG.lastInstant = toInstant(8)
    var delta = 3
    var solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history2,delta,false)
    assert(solver.optimalMappingCost==2)
    val optimalMapping = solver.optimalMapping
    assert(optimalMapping.keySet.map(_.timestamp)==history1.history.versions.keySet)
    assert(optimalMapping.flatMap(_._2.borders).toSet.forall(v => history2.history.versions.contains(v.timestamp)))
    val rhsVersionsSorted = history1.history.versionsSorted
    val lhsVersionsSorted = history2.history.versionsSorted
    assert(optimalMapping(rhsVersionsSorted(0))==VersionRange(lhsVersionsSorted(0),lhsVersionsSorted(0)))
    assert(optimalMapping(rhsVersionsSorted(1))==VersionRange(lhsVersionsSorted(0),lhsVersionsSorted(1)))
    assert(optimalMapping(rhsVersionsSorted(2))==VersionRange(lhsVersionsSorted(1),lhsVersionsSorted(3)))
  }
}
