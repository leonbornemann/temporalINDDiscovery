package de.hpi.temporal_ind.data.column.statistics

import de.hpi.temporal_ind.data.column.data.encoded.OrderedEncodedColumnHistory
import de.hpi.temporal_ind.data.ind.{StrictTemporalIND, SimpleTimeWindowTemporalIND}

import java.io.PrintWriter

class TemporalINDStatRow(lhs: OrderedEncodedColumnHistory,
                         rhs: OrderedEncodedColumnHistory,
                         strictTemporalIND: StrictTemporalIND[Long],
                         variant1TemporalIND: SimpleTimeWindowTemporalIND[Long],
                         deltaInDays:Int) {

  def appendToCSVFile(pr:PrintWriter) = {
    val lhsID = lhs.id
    val rhsID = rhs.id
    val totalActiveTime = strictTemporalIND.totalActiveTimeInDays
    val overlapTime = strictTemporalIND.overlapTimeInDays
    //val nonOverlapTime = strictTemporalIND.nonOverlapTimeInDays
    val strictIsValid = strictTemporalIND.isValid
    val variant1IsValid = variant1TemporalIND.isValid
    val csvString = Seq(lhsID,rhsID,totalActiveTime,overlapTime,/*nonOverlapTime,*/strictIsValid,variant1IsValid,deltaInDays).mkString(",")
    pr.println(csvString)
  }

}
object TemporalINDStatRow {

  def appendSchemaToFile(pr:PrintWriter) = {
    pr.println(getSchema.mkString(","))
  }

  def getSchema = {
    Seq("lhsID",
        "rhsID",
        "totalActiveTime",
        "overlapTime",
        //"nonOverlapTime",
        "strictIsValid",
        "variant1IsValid",
        "deltaInDays")
  }
}

