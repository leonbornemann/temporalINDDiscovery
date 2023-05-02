import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ExponentialDecayWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.TINDParameters
import org.scalatest.flatspec.AnyFlatSpec

import java.time.temporal.ChronoUnit

class TemporalINDExponentialDecayTest extends AnyFlatSpec{

  "Different Validation Variants Tested" should "work correctly" in {
    GLOBAL_CONFIG.earliestInstant = toInstant(1)
    GLOBAL_CONFIG.lastInstant = toInstant(48)
    val history1 = toHistory(Map(
      (5, Set("a")),
      (15, Set()),
      (20, Set("b", "d")),
      (28, Set()),
      (40, Set("c")),
      (46, Set()),
    ))
    val history2 = toHistory(Map(
      (6, Set("a")),
      (10, Set()),
      (14, Set("d")),
      (16, Set("b")),
      (24, Set("d")),
      (26, Set()),
      (39, Set("c")),
      (47, Set()),
    ))
    val decayParams = Seq(0.999,0.99,0.9,0.8,0.7,0.6,0.5)
    decayParams.foreach(a => {
      val function = new ExponentialDecayWeightFunction(a, ChronoUnit.NANOS)
      var shiftedTemporalINDCustomFunction = new ShifteddRelaxedCustomFunctionTemporalIND(history1, history2, TINDParameters(0,2,function), ValidationVariant.FULL_TIME_PERIOD)
      println(a,shiftedTemporalINDCustomFunction.absoluteViolationScore)
    })

  }
}
