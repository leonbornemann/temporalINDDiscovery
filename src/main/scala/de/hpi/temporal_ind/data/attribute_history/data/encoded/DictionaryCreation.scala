package de.hpi.temporal_ind.data.attribute_history.data.encoded

import de.hpi.temporal_ind.data.attribute_history.data.original.ColumnHistory

import java.io.File
import scala.collection.mutable

object DictionaryCreation extends App {
  val inputDir = args(0)
  val targetFile = args(1)
  val allValues = new mutable.TreeSet[String]()
  val listOfAllColumnHistories = new File(inputDir).listFiles().sortBy(_.getName).foreach { f =>
    println(s"Reading ${f.getName} - cur value set size: ${allValues.size}")
    val ch = ColumnHistory.fromJsonObjectPerLineFile(f.getAbsolutePath)
    val values = ch.flatMap(_.columnVersions.flatMap(_.values)).toSet
    allValues ++= values
  }
  val dict = collection.mutable.HashMap[String,Long]()
  var curCounter = 0
  allValues.foreach(s => {
    dict.put(s,curCounter)
    curCounter+=1
  })
  Dictionary(dict).toJsonFile(new File(targetFile))
}
