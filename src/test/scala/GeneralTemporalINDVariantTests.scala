import de.hpi.temporal_ind.data.ind.variant4.TimeShiftedRelaxedTemporalIND
import de.hpi.temporal_ind.data.ind.{StrictTemporalIND, SimpleTimeWindowTemporalIND}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.time.temporal.ChronoUnit

class GeneralTemporalINDVariantTests extends AnyFlatSpec{

  def basePath = "src/main/resources/generalTemporalINDTestExamples/"

  private def runTestCase(file: File) = {
    val tc = TemporalINDTestCase.readFromFile(file)
    println(s"Processing ${tc.name}")
    val indVariants = createTemporalINDVariants(tc)
    indVariants
      .zipWithIndex
      .foreach { case (indv, i) => {
        println(indv)
        assert(indv.isValid == tc.shouldBeValidForVariant(i))
      }
      }
  }

  private def createTemporalINDVariants(tc: TemporalINDTestCase) = {
    val nanosPerDay = 86400000000000L
    IndexedSeq(
      new StrictTemporalIND(tc.lhs, tc.rhs),
      new SimpleTimeWindowTemporalIND(tc.lhs, tc.rhs,  nanosPerDay),
      new TimeShiftedRelaxedTemporalIND(tc.lhs, tc.rhs, nanosPerDay,nanosPerDay)
    )
  }

  "Covered early insert in A" should "work as specified" in {
    runTestCase(new File(basePath + "/Covered early insert in A.tsv"))
  }

  "Test Examples - Early insert Before End of Time" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Early insert Before End of Time.tsv"))
  }

  "Test Examples - Equality and Inclusion" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Equality and Inclusion.tsv"))
  }

  "Test Examples - Too Early Insert" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Too Early Insert.tsv"))
  }

  "Test Examples - Multiple different Early Inserts in A and simultaneous deletes." should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Multiple different Early Inserts in A and simultaneous deletes..tsv"))
  }

  "Test Examples - Multiple different Early Inserts in A" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Multiple different Early Inserts in A.tsv"))
  }

  "Test Examples - Late Delete in A" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Late Delete in A.tsv"))
  }

  "Test Examples - Too Early Update" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Too Early Update.tsv"))
  }

  "Test Examples - Switched Value Order in B" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Switched Value Order in B.tsv"))
  }

  "Test Examples - Oscillating Delete and Insert in B" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Oscillating Delete and Insert in B.tsv"))
  }

  "Test Examples - Oscillating Delete and Insert in A" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Oscillating Delete and Insert in A.tsv"))
  }



  "Test Examples - Simultaneous Temporal deletion (long)" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Simultaneous Temporal deletion (long).tsv"))
  }

  "Test Examples - Temporal deletion of B (long)" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Temporal deletion of B (long).tsv"))
  }

  "Test Examples - Temporal deletion of B (brief)" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Temporal deletion of B (brief).tsv"))
  }

  "Test Examples - Erroneous Insert in A" should "work as specified" in {
    runTestCase(new File(basePath + "/Test Examples - Erroneous Insert in A.tsv"))
  }


}
