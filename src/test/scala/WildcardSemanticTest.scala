import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.column.data.original.ValidationVariant
import de.hpi.temporal_ind.data.ind.{SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedINDDynamicProgrammingSolver, TimeShiftedRelaxedTemporalIND}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

class WildcardSemanticTest extends AnyFlatSpec{

  "Time SHifted Complex IND" should "work correctly using non-wildcards" in {
    val history1 = toHistory(Map(
      (10,Set("a")),
      (50,Set("d"))
    ))
    val history2 = toHistory(Map(
      (10,Set("a")),
      (30,Set())
    ))
    GLOBAL_CONFIG.earliestInstant = toInstant(0)
    GLOBAL_CONFIG.lastInstant = toInstant(100)
    var delta = 10
    val timeShiftedIND = new TimeShiftedRelaxedTemporalIND(history1,history2,delta,0,false)
    timeShiftedIND.getOrCeateSolver()
    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(50),toInstant(30),toInstant(100))==50)
    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(10),toInstant(10),toInstant(100))==10)
    timeShiftedIND.getOrCeateSolver().printMatrix
    assert(timeShiftedIND.getOrCeateSolver().optimalMappingCost == 60)
  }

//  "Time SHifted Complex IND" should "work correctly using wildcards" in {
//    val history1 = toHistory(Map(
//      (10,Set("a")),
//      (50,Set("d"))
//    ))
//    val history2 = toHistory(Map(
//      (10,Set("a")),
//      (30,Set())
//    ))
//    val history3 = toHistory(Map(
//      (10,Set("a")),
//      (25,Set("b")),
//      (30,Set())
//    ))
//    GLOBAL_CONFIG.earliestInstant = toInstant(0)
//    GLOBAL_CONFIG.lastInstant = toInstant(100)
//    var delta = 10
//    var timeShiftedIND = new TimeShiftedRelaxedTemporalIND(history1,history2,delta,0,true)
//    timeShiftedIND.getOrCeateSolver()
//    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(50),toInstant(30),toInstant(100))==0)
//    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(10),toInstant(10),toInstant(100))==0)
//    assert(timeShiftedIND.getOrCeateSolver().optimalMappingCost == 0)
//    //with history 3:
//    timeShiftedIND = new TimeShiftedRelaxedTemporalIND(history1,history3,delta,0,true)
//    timeShiftedIND.getOrCeateSolver()
//    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(50),toInstant(30),toInstant(100))==0)
//    assert(timeShiftedIND.getOrCeateSolver().costFunction(toInstant(10),toInstant(10),toInstant(100))==5)
//    timeShiftedIND.getOrCeateSolver().printMatrix()
//    timeShiftedIND.getEpslionOptimizedMapping.foreach(println)
//    assert(timeShiftedIND.getOrCeateSolver().optimalMappingCost == 5) //TODO: fix that!
//  }

  "Time SHifted Simple IND" should "work correctly using wildcards" in {
    val history1 = toHistory(Map(
      (10,Set("a")),
      (50,Set("d"))
    ))
    val history2 = toHistory(Map(
      (10,Set("a")),
      (30,Set())
    ))
    val history3 = toHistory(Map(
      (10,Set("a")),
      (25,Set("b")),
      (30,Set())
    ))
    GLOBAL_CONFIG.earliestInstant = toInstant(0)
    GLOBAL_CONFIG.lastInstant = toInstant(100)
    var delta = 10
    val timeShiftedIND = new SimpleTimeWindowTemporalIND(history1,history2,delta,0,true,ValidationVariant.FULL_TIME_PERIOD)
    assert(timeShiftedIND.absoluteViolationTime == 0)
    val timeShiftedIND2 = new SimpleTimeWindowTemporalIND(history1,history3,delta,0,true,ValidationVariant.FULL_TIME_PERIOD)
    assert(timeShiftedIND2.absoluteViolationTime==5)
  }

  "Simple Relaxed IND" should "work correctly using wildcards" in {
    val history1 = toHistory(Map(
      (10,Set("a")),
      (50,Set("d"))
    ))
    val history2 = toHistory(Map(
      (10,Set("a")),
      (30,Set())
    ))
    val history3 = toHistory(Map(
      (10,Set("a")),
      (25,Set("b")),
      (30,Set()),
      (40,Set("e")),
      (55,Set())
    ))
    GLOBAL_CONFIG.earliestInstant = toInstant(0)
    GLOBAL_CONFIG.lastInstant = toInstant(100)
    val relaxedIND = new SimpleRelaxedTemporalIND(history1,history2,0,true,ValidationVariant.FULL_TIME_PERIOD)
    assert(relaxedIND.absoluteViolationTime == 0)
    val relaxedIND2 = new SimpleRelaxedTemporalIND(history1,history3,0,true,ValidationVariant.FULL_TIME_PERIOD)
    assert(relaxedIND2.absoluteViolationTime==20)
  }

}
