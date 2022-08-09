package de.hpi.temporal_ind.data.column.data.original

import de.hpi.temporal_ind.data.column.data.IndexedColumnHistories
import de.hpi.temporal_ind.data.ind.{SimpleRelaxedTemporalIND, SimpleTimeWindowTemporalIND}
import de.hpi.temporal_ind.data.ind.variant4.TimeShiftedRelaxedTemporalIND
import de.hpi.temporal_ind.data.wikipedia.GLOBAL_CONFIG

import java.io.PrintWriter

case class LabelledINDCandidateStatistics[T <% Ordered[T]](label:String, candidate:INDCandidate[T]){

  def lhs = candidate.lhs
  def rhs = candidate.rhs
  def idCSVString = s"${lhs.pageID},${lhs.tableId},${lhs.id},${rhs.pageID},${rhs.tableId},${rhs.id}"

  def serializeSimpleRelaxedIND(pr: PrintWriter) = {
    val simpleRelaxedTemporalINDWildcardLogic = new SimpleRelaxedTemporalIND[T](lhs,rhs,1L,true)
    val simpleVariantViolationTimeWildcardLogic = simpleRelaxedTemporalINDWildcardLogic.relativeViolationTime
    val simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND[T](lhs,rhs,1L,false)
    val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime
    pr.println(s"$idCSVString,$label,relaxedNoShift,true,0,$simpleVariantViolationTimeWildcardLogic")
    pr.println(s"$idCSVString,$label,relaxedNoShift,false,0,$simpleVariantViolationTime")
  }

  def serializeTimeShiftedComplexRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.foreach(d => {
      val violationTime = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,d,1,false)
        .getOrCeateSolver()
        .optimalMappingRelativeCost
      pr.println(s"$idCSVString,$label,timeShiftedComplex,false,$d,$violationTime")
    })

  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.map(d => {
      val violationTimeNoWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,false)
        .relativeViolationTime
      val violationTimeWildcard = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d,true)
        .relativeViolationTime
      pr.println(s"$idCSVString,$label,timeShiftedSimple,false,$d,$violationTimeNoWildcard")
      pr.println(s"$idCSVString,$label,timeShiftedSimple,true,$d,$violationTimeWildcard")

    })
  }

  def serializeValidityStatistics(pr:PrintWriter) = {
    serializeSimpleRelaxedIND(pr)
    serializeSimpleRelaxedIND(pr)
    val deltas = Seq(GLOBAL_CONFIG.nanosPerDay,
      GLOBAL_CONFIG.nanosPerDay*2,
      GLOBAL_CONFIG.nanosPerDay*5,
      GLOBAL_CONFIG.nanosPerDay*7,
      GLOBAL_CONFIG.nanosPerDay*10,
      GLOBAL_CONFIG.nanosPerDay*30,
      GLOBAL_CONFIG.nanosPerDay*60,
      GLOBAL_CONFIG.nanosPerDay*90,
      GLOBAL_CONFIG.nanosPerDay*365)
    serializeTimeShiftedComplexRelaxedIND(pr,deltas)
    serializeTimeShiftedSimpleRelaxedIND(pr,deltas)

  }

}
object LabelledINDCandidateStatistics{

  def fromCSVLine(index:IndexedColumnHistories,l:String) = {
    val label = l.split(",")(2)
    val indCandidate = INDCandidate.fromCSVLine(index,l)
    LabelledINDCandidateStatistics(label,indCandidate)
  }

  def printCSVSchema(pr:PrintWriter) = {
    //s"${lhs.pageID},${lhs.tableId},${lhs.tableId},${rhs.pageID},${rhs.tableId},${rhs.tableId}"
    val schema = "lhsPageID,lhsTableID,lhsColumnID,rhsPageID,rhsTableID,rhsColumnID,label,scoreName,wildcardLogic,delta,relativeViolationTime"
    pr.println(schema)
  }

}
