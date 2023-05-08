import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND, ValidationVariant}
import de.hpi.temporal_ind.discovery.TINDParameters
import org.scalatest.flatspec.AnyFlatSpec

import java.time.temporal.ChronoUnit

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
    TINDParameters(0,2,new ConstantWeightFunction())
    var simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.FULL_TIME_PERIOD)
    var shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,new ConstantWeightFunction()),ValidationVariant.FULL_TIME_PERIOD)
    assert(simpleRelaxedTemporalIND.absoluteViolationScore == 3+2+2)
    assert(simpleRelaxedTemporalIND.denominator == 47)
    assert(simpleRelaxedTemporalIND.absoluteViolationScore==shiftedTemporalINDCustomFunction.absoluteViolationScore)
    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_ONLY)
//    shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,new ConstantWeightFunction()),ValidationVariant.LHS_ONLY)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore == 3 + 2 + 2)
//    assert(simpleRelaxedTemporalIND.denominator == 10 + 8 + 6)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore==shiftedTemporalINDCustomFunction.absoluteViolationScore)
//    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.RHS_ONLY)
//    shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,new ConstantWeightFunction()),ValidationVariant.RHS_ONLY)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore == 1+2)
//    assert(simpleRelaxedTemporalIND.denominator == 4 + 12 + 8)
//    val debug = (simpleRelaxedTemporalIND.relevantTimestamps, shiftedTemporalINDCustomFunction.relevantValidationIntervals)
//    val a = debug._1.map(_._1) == debug._2.map(_._1)
//    val timesSimple = simpleRelaxedTemporalIND.debugViolationTimes
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore==shiftedTemporalINDCustomFunction.absoluteViolationScore)
//    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_UNION_RHS)
//    shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,new ConstantWeightFunction()),ValidationVariant.LHS_UNION_RHS)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore == 3 + 2 + 2)
//    assert(simpleRelaxedTemporalIND.denominator == 23 + 8)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore==shiftedTemporalINDCustomFunction.absoluteViolationScore)
//    simpleRelaxedTemporalIND = new SimpleTimeWindowTemporalIND(history1, history2, 2, 0,false, ValidationVariant.LHS_INTERSECT_RHS)
//    shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,new ConstantWeightFunction()),ValidationVariant.LHS_INTERSECT_RHS)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore == 1+2)
//    assert(simpleRelaxedTemporalIND.denominator == 4 +1+ 6 + 6)
//    assert(simpleRelaxedTemporalIND.absoluteViolationScore==shiftedTemporalINDCustomFunction.absoluteViolationScore)
  }

}
