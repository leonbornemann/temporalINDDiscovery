import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.ind.{SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

class SimpleShiftedTemporalINDTest extends AnyFlatSpec{

  "Different Validation Variants Tested" should "work correctly" in {
    GLOBAL_CONFIG.earliestInstant = toInstant(1)
    GLOBAL_CONFIG.lastInstant = toInstant(48)
    val history1 = toHistory(Map(
      (5, Set("a")),
      (15, Set()),
      (20, Set("b","d")),
      (28, Set()),
      (40, Set("c")),
      (46, Set()),
    ))
    val history2 = toHistory(Map(
      (6, Set("a")),
      (10, Set()),
      (14,Set("d")),
      (16, Set("b")),
      (24, Set("d")),
      (26,Set()),
      (39, Set("c")),
      (47, Set()),
    ))
    var simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.FULL_TIME_PERIOD)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 3+2+2)
    assert(simpleRelaxedTemporalIND.denominator == 47)
    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_ONLY)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 3 + 2 + 2)
    assert(simpleRelaxedTemporalIND.denominator == 10 + 8 + 6)
    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.RHS_ONLY)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 1+2)
    assert(simpleRelaxedTemporalIND.denominator == 4 + 12 + 8)
    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_UNION_RHS)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 3 + 2 + 2)
    assert(simpleRelaxedTemporalIND.denominator == 23 + 8)
    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_INTERSECT_RHS)
    assert(simpleRelaxedTemporalIND.absoluteViolationTime == 1+2)
    assert(simpleRelaxedTemporalIND.denominator == 4 +1+ 6 + 6)
  }

}
