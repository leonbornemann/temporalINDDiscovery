import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
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
    var simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history2,7,false,ValidationVariant.FULL_TIME_PERIOD)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime==5)
    assert(simpleRelaxedTemporalIND.isValid)
    val history3 = toHistory(Map(
      (1,Set("a","b")),
      (10,Set("e","f")),
      (20,Set("a","f")),
      (30,Set("e","f"))
    ))
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history3,7,false,ValidationVariant.FULL_TIME_PERIOD)
    assert(!simpleRelaxedTemporalIND.isValid)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime==17)
  }

  "Different Validation Variants Tested" should "work correctly" in {
    GLOBAL_CONFIG.earliestInstant = toInstant(1)
    GLOBAL_CONFIG.lastInstant = toInstant(48 )
    val history1 = toHistory(Map(
      (5,Set("a")),
      (15,Set()),
      (20, Set("b")),
      (28, Set()),
      (40, Set("c")),
      (46, Set()),
    ))
    val history2 = toHistory(Map(
      (6, Set("a")),
      (10, Set()),
      (16, Set("b")),
      (24, Set()),
      (39, Set("c")),
      (47, Set()),
    ))
    var simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history2,7,false,ValidationVariant.FULL_TIME_PERIOD)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime==1+5+4)
    assert(simpleRelaxedTemporalIND.denominator==47)
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1,history2,7,false,ValidationVariant.LHS_ONLY)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime==1+5+4)
    assert(simpleRelaxedTemporalIND.denominator==10+8+6)
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1, history2, 7, false, ValidationVariant.RHS_ONLY)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 0)
    assert(simpleRelaxedTemporalIND.denominator==4+8+8)
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1, history2, 7, false, ValidationVariant.LHS_UNION_RHS)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 1 + 5 + 4)
    assert(simpleRelaxedTemporalIND.denominator==10+12+8)
    simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND(history1, history2, 7, false, ValidationVariant.LHS_INTERSECT_RHS)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 0)
    assert(simpleRelaxedTemporalIND.denominator==4+4+6)
  }

}
