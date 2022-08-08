import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.ind.SimpleRelaxedTemporalIND
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedINDDynamicProgrammingSolver, VersionRange}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

class SimpleRelaxedTemporalINDTest extends AnyFlatSpec{

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
    GLOBAL_CONFIG.lastInstant = toInstant(40)
    var simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history2,7)
    assert(simpleRelaxedTemporalIND.summedViolationTime==5)
    assert(simpleRelaxedTemporalIND.isValid)
    val history3 = toHistory(Map(
      (1,Set("a","b")),
      (10,Set("e","f")),
      (20,Set("a","f")),
      (30,Set("e","f"))
    ))
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history3,7)
    assert(!simpleRelaxedTemporalIND.isValid)
    assert(simpleRelaxedTemporalIND.summedViolationTime==17)
  }

}
