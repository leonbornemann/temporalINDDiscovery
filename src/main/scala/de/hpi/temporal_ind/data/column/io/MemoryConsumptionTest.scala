package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.data.encoded.ColumnHistoryEncoded
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory

import java.io.File
import scala.io.Source

object MemoryConsumptionTest extends App {

  val inputDir = args(0)
  val listOfAllColumnHistories = collection.mutable.TreeMap[String,ColumnHistoryEncoded]()
  var collisions = 0
  new File(inputDir).listFiles().foreach { f =>
    println(s"Reading ${f.getName}")
    val histories = ColumnHistoryEncoded.fromJsonObjectPerLineFile(f.getAbsolutePath)
    histories.foreach(h => {
      if(listOfAllColumnHistories.contains(h.id)){
        collisions +=1
      }
      listOfAllColumnHistories.put(h.id,h)
    })
  }
  println(s"Size: ${listOfAllColumnHistories.size} - num Collisions: $collisions")
  println("Enter anything to terminate")
  val res = io.StdIn.readLine()
  println(s"Entered $res")

}
