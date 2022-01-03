package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.ColumnHistory
import de.hpi.temporal_ind.data.column.io.MemoryConsumptionTest.args

import java.io.File

object DictionaryCreation extends App {

  val inputDir = args(0)
  val targetFile = args(1)
  val allValues = collection.mutable.HashMap[String,Long]()
  var curCounter = 0
  val listOfAllColumnHistories = new File(inputDir).listFiles().sortBy(_.getName).foreach { f =>
    println(s"Reading ${f.getName}")
    val ch = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val values = ch.flatMap(_.columnVersions.flatMap(_.values)).toSet
    values.foreach(v => {
      if(!allValues.contains(v)){
        allValues.put(v,curCounter)
        curCounter+=1
      }
    })
  }
  Dictionary(allValues).toJsonFile(new File(targetFile))
}
