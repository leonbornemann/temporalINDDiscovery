package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.{IncrementalIndexedColumnHistories, IndexedColumnHistories}
import de.hpi.temporal_ind.data.ind.{SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND, StrictTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.{TimeShiftedRelaxedTemporalIND, TimeUtil}
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.PrintWriter

case class LabelledINDCandidateStatistics[T <% Ordered[T]](label:String, candidate:INDCandidate[T]){

  def lhs = candidate.lhs
  def rhs = candidate.rhs
  def idCSVString = s"${lhs.pageID},${lhs.tableId},${lhs.id},${rhs.pageID},${rhs.tableId},${rhs.id}"

  def simpleAndComplexAreDifferentForDelta(delta:Long) = {
    val violationTimeSimple = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,delta,1,false)
      .absoluteViolationTime
    val violationTimeComplex = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,delta,1,false)
      .absoluteViolationTime
    violationTimeSimple!=violationTimeComplex
  }

  def serializeSimpleRelaxedIND(pr: PrintWriter, validationTypes: ValidationVariant.ValueSet) = {
    for(validationType <- validationTypes) {
      val simpleRelaxedTemporalINDWildcardLogic = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, true,validationType)
      val simpleVariantViolationTimeWildcardLogic = simpleRelaxedTemporalINDWildcardLogic.relativeViolationTime()
      val simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, false,validationType)
      val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime()
      pr.println(s"$idCSVString,$label,relaxedNoShift,true,$validationType,0,$simpleVariantViolationTimeWildcardLogic")
      pr.println(s"$idCSVString,$label,relaxedNoShift,false,$validationType,0,$simpleVariantViolationTime")
    }
  }

  def serializeTimeShiftedComplexRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.foreach(d => {
      val violationTime = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,d,1,false)
        .relativeViolationTime()
      pr.println(s"$idCSVString,$label,timeShiftedComplex,false,${ValidationVariant.FULL_TIME_PERIOD},$d,$violationTime")
    })
  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, deltas: Seq[Long], validationTypes: ValidationVariant.ValueSet) = {
    for(validationType <- validationTypes) {
      deltas.map(d => {
        val violationTimeNoWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,false,validationType)
          .relativeViolationTime()
        val violationTimeWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,true,validationType)
          .relativeViolationTime()
        pr.println(s"$idCSVString,$label,timeShiftedSimple,false,$validationType,$d,$violationTimeNoWildcard")
        pr.println(s"$idCSVString,$label,timeShiftedSimple,true,$validationType,$d,$violationTimeWildcard")
      })
    }
  }

  def serializeStrictTemporalIND(pr: PrintWriter) = {
    for(validationType <- ValidationVariant.values) {
      val strictTINDWildcardIsValid = new StrictTemporalIND[T](lhs, rhs, true, validationType).isValid
      val strictTINDIsValid = new StrictTemporalIND[T](lhs, rhs, false, validationType).isValid
      val scoreWildcard = if(strictTINDWildcardIsValid) 0 else 1
      val score = if(strictTINDIsValid) 0 else 1
      pr.println(s"$idCSVString,$label,strict,true,$validationType,0,$scoreWildcard")
      pr.println(s"$idCSVString,$label,strict,false,$validationType,0,$score")
    }
  }

  def serializeValidityStatistics(pr:PrintWriter) = {
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
    serializeTimeShiftedComplexRelaxedIND(pr,deltas)
    serializeTimeShiftedSimpleRelaxedIND(pr,deltas,normalizationTypes)

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
    val schema = "lhsPageID,lhsTableID,lhsColumnID,rhsPageID,rhsTableID,rhsColumnID,label,scoreName,wildcardLogic,normalizationVariant,delta,relativeViolationTime"
    pr.println(schema)
  }

}
