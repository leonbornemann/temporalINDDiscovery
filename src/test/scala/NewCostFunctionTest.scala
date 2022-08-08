import TestUtilMethods.{toHistory, toInstant}
import de.hpi.temporal_ind.data.column.data.original.{ColumnVersion, OrderedColumnHistory, OrderedColumnVersionList}
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedINDDynamicProgrammingSolver, TimeShiftedRelaxedTemporalIND}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.time.Instant

class NewCostFunctionTest extends AnyFlatSpec{

  "Large Window test" should "compute correct costs" in {
    val history1 = toHistory(Map(
      (10,Set("a","b","c")),
      (20,Set("d"))
    ))
    val history2 = toHistory(Map(
      (10,Set("a","b","c")),
      (20,Set("b"))
    ))
    val history3 = toHistory(Map(
      (10,Set("d")),
      (20,Set("e"))
    ))
    val history4 = toHistory(Map(
      (7,Set("a","b","c")),
      (10,Set("e")),
      (20,Set("f"))
    ))
    val history5 = toHistory(Map(
      (5,Set("a","b")),
      (6,Set("c")),
      (24,Set("a","b")),
      (25,Set("a","b","e")),
      (26,Set("a","b"))
    ))
    val history6 = toHistory(Map(
      (10,Set("d")),
      (20,Set("a","b","c"))
    ))
    GLOBAL_CONFIG.lastInstant = toInstant(30)
    val delta = 5
    var solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history2,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),toInstant(20))==0)
    solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history3,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),toInstant(20))==10)
    solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history4,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),toInstant(20))==10)
    assert(solver.costFunction(toInstant(10),toInstant(7),toInstant(20))==7)
    solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history5,delta)
    assert(solver.costFunction(toInstant(10),toInstant(5),toInstant(6))==10)
    assert(solver.costFunction(toInstant(10),toInstant(5),toInstant(25))==8)
    assert(solver.costFunction(toInstant(10),toInstant(6),toInstant(25))==9)
    assert(solver.costFunction(toInstant(10),toInstant(5),toInstant(26))==8)
    assert(solver.costFunction(toInstant(10),toInstant(5),GLOBAL_CONFIG.lastInstant)==8)
    solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history6,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),toInstant(20))==10)
    assert(solver.costFunction(toInstant(10),toInstant(10),GLOBAL_CONFIG.lastInstant)==5)
  }

  "Small Window test" should "compute correct costs" in {
    val history1 = toHistory(Map(
      (10,Set("a","b","c")),
      (25,Set("d"))
    ))
    val history2 = toHistory(Map(
      (10,Set("a","b","c")),
      (11,Set("e")),
      (13,Set("a","b","c")),
      (14,Set("e")),
      (17,Set("a","b","c"))
    ))
    GLOBAL_CONFIG.lastInstant = toInstant(30)
    var delta = 2
    var solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history2,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),GLOBAL_CONFIG.lastInstant)==0)
    delta = 1
    solver = new TimeShiftedRelaxedINDDynamicProgrammingSolver(history1,history2,delta)
    assert(solver.costFunction(toInstant(10),toInstant(10),GLOBAL_CONFIG.lastInstant)==1)
    assert(solver.costFunction(toInstant(10),toInstant(17),GLOBAL_CONFIG.lastInstant)==6)
    assert(solver.costFunction(toInstant(10),toInstant(10),toInstant(17))==10)
  }

}
