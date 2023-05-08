package de.hpi.temporal_ind.data.attribute_history

import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory

import java.io.File
import java.time.temporal.ChronoUnit

object IntegrityCheckMain extends App {
  val dir = new File(args(0))
  ColumnHistory.iterableFromJsonObjectPerLineDir(dir,true)
    .foreach(ch => {
      val withIndex = ch.columnVersions.zipWithIndex
      assert(withIndex.forall{case (cv,i) => i==0 || cv.values!= withIndex(i-1)._1.values})
      assert(withIndex.forall{case (cv,i) => i==withIndex.size-1 || ChronoUnit.DAYS.between(cv.timestamp,withIndex(i+1)._1.timestamp)>=1})
      assert(ch.columnVersions.exists(cv => !cv.values.isEmpty))
    })
}
