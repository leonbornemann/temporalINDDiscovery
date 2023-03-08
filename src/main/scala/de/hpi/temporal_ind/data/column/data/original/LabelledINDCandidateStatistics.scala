package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.ind.{ConstantWeightFunction, ExponentialDecayWeightFunction, ShifteddRelaxedCustomFunctionTemporalIND, SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND, StrictTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedTemporalIND, TimeUtil}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.PrintWriter
import java.time.temporal.ChronoUnit

case class LabelledINDCandidateStatistics[T <% Ordered[T]](label:String, candidate:INDCandidate[T]){

  def lhs = candidate.lhs
  def rhs = candidate.rhs
  def idCSVString = s"${lhs.pageID},${lhs.tableId},${lhs.id},${rhs.pageID},${rhs.tableId},${rhs.id}"

  def simpleAndComplexAreDifferentForDelta(delta:Long) = {
    val violationTimeSimple = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,delta,1,false)
      .absoluteViolationScore
    val violationTimeComplex = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,delta,1,false)
      .absoluteViolationScore
    violationTimeSimple!=violationTimeComplex
  }

  def serializeSimpleRelaxedIND(pr: PrintWriter, validationTypes: ValidationVariant.ValueSet) = {
    for(validationType <- validationTypes) {
      val simpleRelaxedTemporalINDWildcardLogic = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, true,validationType)
      val simpleVariantViolationTimeWildcardLogic = simpleRelaxedTemporalINDWildcardLogic.relativeViolationTime()
      val simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, false,validationType)
      val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime()
      pr.println(s"$idCSVString,$label,relaxedNoShift,true,$validationType,0,1,$simpleVariantViolationTimeWildcardLogic")
      pr.println(s"$idCSVString,$label,relaxedNoShift,false,$validationType,0,1,$simpleVariantViolationTime")
    }
  }

  def serializeTimeShiftedComplexRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.foreach(d => {
      val violationTime = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,d,1,false)
        .relativeViolationTime()
      pr.println(s"$idCSVString,$label,timeShiftedComplex,false,${ValidationVariant.FULL_TIME_PERIOD},$d,1,$violationTime")
    })
  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, d: Long, validationTypes: ValidationVariant.ValueSet) = {
    for(validationType <- validationTypes) {
      val violationTimeNoWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,0,false,validationType)
        .relativeViolationTime()
      val violationTimeWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,0,true,validationType)
        .relativeViolationTime()
      pr.println(s"$idCSVString,$label,timeShiftedSimple,false,$validationType,$d,1,$violationTimeNoWildcard")
      pr.println(s"$idCSVString,$label,timeShiftedSimple,true,$validationType,$d,1,$violationTimeWildcard")
    }
  }

  def serializeTimeShiftedExponentialDecayedIND(pr: PrintWriter, d: Long, validationTypes: ValidationVariant.ValueSet,alphas:Seq[Double]) = {
    for (validationType <- validationTypes) {
      alphas.foreach(a => {
        val decayFunction = new ExponentialDecayWeightFunction(a,ChronoUnit.DAYS)
        val violationTimeNoWildcard = new ShifteddRelaxedCustomFunctionTemporalIND[T](lhs, rhs, d, 0, decayFunction, validationType)
          .relativeViolationTime()
        pr.println(s"$idCSVString,$label,timeShiftedExponentialDecay,false,$validationType,$d,$a,$violationTimeNoWildcard")
      })
    }
  }

  def serializeStrictTemporalIND(pr: PrintWriter) = {
    for(validationType <- ValidationVariant.values) {
      val strictTINDWildcardIsValid = new StrictTemporalIND[T](lhs, rhs, true, validationType).isValid
      val strictTINDIsValid = new StrictTemporalIND[T](lhs, rhs, false, validationType).isValid
      val scoreWildcard = if(strictTINDWildcardIsValid) 0 else 1
      val score = if(strictTINDIsValid) 0 else 1
      pr.println(s"$idCSVString,$label,strict,true,$validationType,0,1,$scoreWildcard")
      pr.println(s"$idCSVString,$label,strict,false,$validationType,0,1,$score")
    }
  }

  def serializeValidityStatistics(pr:PrintWriter,prTimeStats:Option[PrintWriter]) = {
    val normalizationTypes = ValidationVariant.values
    serializeStrictTemporalIND(pr)
    serializeSimpleRelaxedIND(pr,normalizationTypes)
    val deltas = Seq(TimeUtil.nanosPerDay,
      TimeUtil.nanosPerDay*2,
      TimeUtil.nanosPerDay*5,
      TimeUtil.nanosPerDay*7,
      TimeUtil.nanosPerDay*10,
      TimeUtil.nanosPerDay*30,
      TimeUtil.nanosPerDay*60,
      TimeUtil.nanosPerDay*90,
      TimeUtil.nanosPerDay*365)
    val alphas = Seq(
      0.99924035959937,
      0.99946897341668,
      0.99960272817285,
      0.999697639538,
      0.99977126471818,
      0.99983142489561,
      0.99988229242513,
      0.9999263579753,
      0.99996522815714
    )
    //serializeTimeShiftedComplexRelaxedIND(pr,deltas)
    deltas.foreach(d => {
      val (simpleRelaxed,timeSimple) = TimeUtil.executionTimeInMS(new SimpleTimeWindowTemporalIND(lhs,rhs,d,0,false,ValidationVariant.FULL_TIME_PERIOD))
      val (shifteNew,timeNew) = TimeUtil.executionTimeInMS(new ShifteddRelaxedCustomFunctionTemporalIND(lhs,rhs,d,0,new ConstantWeightFunction(ChronoUnit.DAYS),ValidationVariant.FULL_TIME_PERIOD))
      if(!(shifteNew.absoluteViolationScore == simpleRelaxed.absoluteViolationScore)){
        println("Difference between simpleRelaxed",simpleRelaxed.absoluteViolationScore)
        println("and new Variant",shifteNew.absoluteViolationScore)
        println("For RHS ",lhs.pageID,lhs.tableId,lhs.id)
        println("And LHS ",rhs.pageID,rhs.tableId,rhs.id)
      }
      if(prTimeStats.isDefined)
        prTimeStats.get.println(s"$timeSimple,$timeNew")
      serializeTimeShiftedSimpleRelaxedIND(pr, d, normalizationTypes)
      serializeTimeShiftedExponentialDecayedIND(pr, d, normalizationTypes, alphas)
    })
    pr.flush()
  }

}
object LabelledINDCandidateStatistics{

  def fromCSVLine(index:IndexedColumnHistories,l:String) = {
    val label = l.split(",")(8)
    val indCandidate = INDCandidate.fromCSVLine(index,l)
    LabelledINDCandidateStatistics(label,indCandidate)
  }

  def fromCSVLineWithIncrementalIndex(index: IncrementalIndexedColumnHistories, l: String) = {
    val label = l.split(",")(8)
    val indCandidate = INDCandidate.fromCSVLineWithIncrementalIndex(index, l)
    LabelledINDCandidateStatistics(label, indCandidate)
  }

  def printCSVSchema(pr:PrintWriter) = {
    //s"${lhs.pageID},${lhs.tableId},${lhs.tableId},${rhs.pageID},${rhs.tableId},${rhs.tableId}"
    val schema = "lhsPageID,lhsTableID,lhsColumnID,rhsPageID,rhsTableID,rhsColumnID,label,scoreName,wildcardLogic,normalizationVariant,delta,alpha,relativeViolationTime"
    pr.println(schema)
  }

}
