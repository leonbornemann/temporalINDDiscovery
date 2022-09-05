package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.ind.{SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND}
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

  def serializeSimpleRelaxedIND(pr: PrintWriter,normalizationTypes: NormalizationVariant.ValueSet) = {
    for(normalizationVariant <- normalizationTypes) {
      val simpleRelaxedTemporalINDWildcardLogic = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, true)
      val simpleVariantViolationTimeWildcardLogic = simpleRelaxedTemporalINDWildcardLogic.relativeViolationTime(normalizationVariant)
      val simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND[T](lhs, rhs, 1L, false)
      val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime(normalizationVariant)
      pr.println(s"$idCSVString,$label,relaxedNoShift,true,$normalizationVariant,0,$simpleVariantViolationTimeWildcardLogic")
      pr.println(s"$idCSVString,$label,relaxedNoShift,false,$normalizationVariant,0,$simpleVariantViolationTime")
    }
  }

  def serializeTimeShiftedComplexRelaxedIND(pr: PrintWriter, deltas: Seq[Long],normalizationTypes: NormalizationVariant.ValueSet) = {
    for(normalizationVariant <- normalizationTypes){
      deltas.foreach(d => {
        val violationTime = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,d,1,false)
          .relativeViolationTime(normalizationVariant)
        pr.println(s"$idCSVString,$label,timeShiftedComplex,false,$normalizationVariant,$d,$violationTime")
      })
    }
  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, deltas: Seq[Long],normalizationTypes: NormalizationVariant.ValueSet) = {
    for(normalizationVariant <- normalizationTypes) {
      deltas.map(d => {
        val violationTimeNoWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,false)
          .relativeViolationTime(normalizationVariant)
        val violationTimeWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,true)
          .relativeViolationTime(normalizationVariant)
        pr.println(s"$idCSVString,$label,timeShiftedSimple,false,$normalizationVariant,$d,$violationTimeNoWildcard")
        pr.println(s"$idCSVString,$label,timeShiftedSimple,true,$normalizationVariant,$d,$violationTimeWildcard")
      })
    }
  }

  def serializeValidityStatistics(pr:PrintWriter) = {
    val normalizationTypes = NormalizationVariant.values
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
    serializeTimeShiftedComplexRelaxedIND(pr,deltas,normalizationTypes)
    serializeTimeShiftedSimpleRelaxedIND(pr,deltas,normalizationTypes)

  }

}
object LabelledINDCandidateStatistics{

  def fromCSVLine(index:IndexedColumnHistories,l:String) = {
    val label = l.split(",")(8)
    val indCandidate = INDCandidate.fromCSVLine(index,l)
    LabelledINDCandidateStatistics(label,indCandidate)
  }

  def printCSVSchema(pr:PrintWriter) = {
    //s"${lhs.pageID},${lhs.tableId},${lhs.tableId},${rhs.pageID},${rhs.tableId},${rhs.tableId}"
    val schema = "lhsPageID,lhsTableID,lhsColumnID,rhsPageID,rhsTableID,rhsColumnID,label,scoreName,wildcardLogic,normalizationVariant,delta,relativeViolationTime"
    pr.println(schema)
  }

}
