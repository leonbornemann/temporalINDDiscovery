import de.hpi.temporal_ind.data.ind.variant4.{Variant4TemporalIND, Variant4_1_CostFunction}
import de.hpi.temporal_ind.data.ind.{StrictTemporalIND, Variant1TemporalIND, Variant3TemporalIND}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File

class TemporalINDVariantTests extends AnyFlatSpec{

  def basePath = "src/main/resources/temporalINDTestExamples/"

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
    IndexedSeq(
      new StrictTemporalIND(tc.lhs, tc.rhs),
      new Variant1TemporalIND(tc.lhs, tc.rhs, 1),
      new Variant3TemporalIND(tc.lhs, tc.rhs, 1),
      new Variant4TemporalIND(tc.lhs, tc.rhs, 1,1,new Variant4_1_CostFunction)
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
