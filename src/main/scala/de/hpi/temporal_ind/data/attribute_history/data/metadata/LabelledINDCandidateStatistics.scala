package de.hpi.temporal_ind.data.attribute_history.data.metadata

import de.hpi.temporal_ind.data.attribute_history.data.file_search.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.ind._
import de.hpi.temporal_ind.data.ind.weight_functions.{ConstantWeightFunction, ExponentialDecayWeightFunction}
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.util.TimeUtil

import java.io.PrintWriter
import java.time.temporal.ChronoUnit

case class LabelledINDCandidateStatistics[T <% Ordered[T]](label:String, candidate:INDCandidate[T]){

  def lhs = candidate.lhs
  def rhs = candidate.rhs
  def idCSVString = s"${lhs.pageID},${lhs.tableId},${lhs.id},${rhs.pageID},${rhs.tableId},${rhs.id}"

  def serializeSimpleRelaxedIND(pr: PrintWriter, validationTypes: IndexedSeq[ValidationVariant.Value]) = {
    for(validationType <- validationTypes) {
      val simpleRelaxedTemporalINDWildcardLogic = new EpsilonRelaxedTemporalIND[T](lhs, rhs, 1L, true,validationType)
      val simpleVariantViolationTimeWildcardLogic = simpleRelaxedTemporalINDWildcardLogic.relativeViolationTime()
      val simpleRelaxedTemporalIND = new EpsilonRelaxedTemporalIND[T](lhs, rhs, 1L, false,validationType)
      val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime()
      pr.println(s"$idCSVString,$label,relaxedNoShift,true,$validationType,0,1,$simpleVariantViolationTimeWildcardLogic")
      pr.println(s"$idCSVString,$label,relaxedNoShift,false,$validationType,0,1,$simpleVariantViolationTime")
    }
  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, d: Long, validationTypes: IndexedSeq[ValidationVariant.Value]) = {
    for(validationType <- validationTypes) {
      val violationTimeNoWildcard = new EpsilonOmegaDeltaRelaxedTemporalIND(lhs,rhs,TINDParameters(0,d,new ConstantWeightFunction()),ValidationVariant.FULL_TIME_PERIOD)
        .relativeViolationTime()
      pr.println(s"$idCSVString,$label,timeShiftedSimple,false,$validationType,$d,1,$violationTimeNoWildcard")
    }
  }

  def serializeTimeShiftedExponentialDecayedIND(pr: PrintWriter, d: Long, validationTypes: IndexedSeq[ValidationVariant.Value],alphas:Seq[Double]) = {
    for (validationType <- validationTypes) {
      alphas.foreach(a => {
        val decayFunction = new ExponentialDecayWeightFunction(a,ChronoUnit.DAYS)
        val violationTimeNoWildcard = new EpsilonOmegaDeltaRelaxedTemporalIND[T](lhs, rhs, TINDParameters(0,d,decayFunction), validationType)
          .relativeViolationTime()
        pr.println(s"$idCSVString,$label,timeShiftedExponentialDecay,false,$validationType,$d,$a,$violationTimeNoWildcard")
      })
      //add alpha 1:
      val decayFunction = new ConstantWeightFunction()
      val violationTimeNoWildcard = new EpsilonOmegaDeltaRelaxedTemporalIND[T](lhs, rhs, TINDParameters(0,d,decayFunction), validationType)
        .relativeViolationTime()
      pr.println(s"$idCSVString,$label,timeShiftedExponentialDecay,false,$validationType,$d,1,$violationTimeNoWildcard")
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
    val normalizationTypes = IndexedSeq(ValidationVariant.FULL_TIME_PERIOD)
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
      val (simpleRelaxed,timeSimple) = TimeUtil.executionTimeInMS(new EpsilonDeltaRelaxedTemporalIND(lhs,rhs,d,0,false,ValidationVariant.FULL_TIME_PERIOD))
      val (shifteNew,timeNew) = TimeUtil.executionTimeInMS(new EpsilonOmegaDeltaRelaxedTemporalIND(lhs,rhs,TINDParameters(0,d,new ConstantWeightFunction()),ValidationVariant.FULL_TIME_PERIOD))
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
