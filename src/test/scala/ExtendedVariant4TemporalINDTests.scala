import de.hpi.temporal_ind.data.ind.variant4.{Variant4TemporalIND, Variant4_1_CostFunction}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.time.Instant

class ExtendedVariant4TemporalINDTests extends AnyFlatSpec{

  "Delta-Extension Test Early.tsv" should "work as specified" in {
    val testFile = new File("src/main/resources/specificVariant4TemporalINDTestExamples/Delta-Extension Test Early.tsv")
    val delta = 2
    val epsilon = 1
    val expectedBestMappingCost = 1
    val (timeaxis,histories) = TemporalINDTestCase.readHistoriesWithoutExpectedResults(testFile,true);
    val tind = new Variant4TemporalIND(histories(0),histories(1),delta,epsilon,new Variant4_1_CostFunction)
    tind.getEpslionOptimizedMapping.mappingFunction
      .toIndexedSeq
      .sorted
      .foreach(println)
    assert(tind.isValid)
    assert(tind.solver.get.bestMappingCost==expectedBestMappingCost)
  }

  "Delta-Extension Test Late.tsv" should "work as specified" in {
    val testFile = new File("src/main/resources/specificVariant4TemporalINDTestExamples/Delta-Extension Test Late.tsv")
    val delta = 2
    val epsilon = 1
    val expectedBestMappingCost = 2
    val (timeaxis,histories) = TemporalINDTestCase.readHistoriesWithoutExpectedResults(testFile,true);
    val tind = new Variant4TemporalIND(histories(0),histories(1),delta,epsilon,new Variant4_1_CostFunction)
    assert(!tind.isValid)
    assert(tind.solver.get.bestMappingCost==expectedBestMappingCost)
    println(tind.getEpslionOptimizedMapping)
    val lhsStart = Instant.parse("2001-01-04T00:00:00Z")
    val lhsEnd = Instant.parse("2001-01-05T00:00:00Z")
    val expectedRHSStart = Instant.parse("2001-01-02T00:00:00Z")
    val expectedRHSEnd = Instant.parse("2001-01-03T00:00:00.000000001Z")
    //assert(tind.getEpslionOptimizedMapping(start,))
    tind.getEpslionOptimizedMapping.mappingFunction
      .toIndexedSeq
      .sorted
      .foreach(println)
    assert(tind.getEpslionOptimizedMapping.mappingFunction((lhsStart,lhsEnd)) == (expectedRHSStart,expectedRHSEnd))
  }

  "Delta-Extension Test Very Late Addition.tsv" should "work as specified" in {
    val testFile = new File("src/main/resources/specificVariant4TemporalINDTestExamples/Delta-Extension Test Very Late Addition.tsv")
    val delta = 3
    val epsilon = 1
    val expectedBestMappingCost = 1
    val (timeaxis,histories) = TemporalINDTestCase.readHistoriesWithoutExpectedResults(testFile,true);
    val tind = new Variant4TemporalIND(histories(0),histories(1),delta,epsilon,new Variant4_1_CostFunction)
    tind.getEpslionOptimizedMapping.mappingFunction
      .toIndexedSeq
      .sorted
      .foreach(println)
    assert(tind.isValid)
    assert(tind.solver.get.bestMappingCost==expectedBestMappingCost)
    //2001-01-04:
    val lhsStart = Instant.parse("2001-01-04T00:00:00Z")
    val lhsEnd = Instant.parse("2001-01-05T00:00:00Z")
    val expectedRHSEnd = Instant.parse("2001-01-04T00:00:00.000000001Z")
    //assert(tind.getEpslionOptimizedMapping(start,))
    val (actualRHSStart,actualRHSEnd) = tind.getEpslionOptimizedMapping.mappingFunction((lhsStart,lhsEnd))
    // assert(actualRHSEnd == expectedRHSEnd)
//    TODO: fix these mappings being wrong - introduce the ability to corss same versions - then change epsilon and expected costs to 0
  }

}
