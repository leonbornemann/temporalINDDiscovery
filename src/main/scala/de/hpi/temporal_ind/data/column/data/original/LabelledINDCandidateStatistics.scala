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
    val simpleRelaxedTemporalIND = new SimpleRelaxedTemporalIND[T](lhs,rhs,1L)
    val simpleVariantViolationTime = simpleRelaxedTemporalIND.relativeViolationTime
    pr.println(s"$idCSVString,relaxedNoShift,0,$simpleVariantViolationTime")
  }

  def serializeTimeShiftedComplexRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.foreach(d => {
      val timeShiftedIND = new TimeShiftedRelaxedTemporalIND[T](lhs,rhs,d,1)
      val violationTime = timeShiftedIND.getOrCeateSolver().optimalMappingRelativeCost
      pr.println(s"$idCSVString,timeShiftedComplex,$d,$violationTime")
    })

  }

  def serializeTimeShiftedSimpleRelaxedIND(pr: PrintWriter, deltas: Seq[Long]) = {
    deltas.map(d => {
      val timeShiftedIND = new SimpleTimeWindowTemporalIND[T](lhs,rhs,d)
      val violationTime:Double = timeShiftedIND.relativeViolationTime
      pr.println(s"$idCSVString,timeShiftedSimple,$d,$violationTime")

    })
  }

  def serializeValidityStatistics(pr:PrintWriter) = {
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
    val schema = "lhsPageID,lhsTableID,lhsColumnID,rhsPageID,rhsTableID,rhsColumnID,label,scoreName,delta,relativeViolationTime"
    pr.println("")
  }

}
