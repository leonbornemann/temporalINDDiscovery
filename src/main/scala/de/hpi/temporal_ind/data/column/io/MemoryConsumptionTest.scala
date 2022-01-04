package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.{ColumnHistory, ColumnHistoryEncoded}

import java.io.File
import scala.io.Source

object MemoryConsumptionTest extends App {

  val inputDir = args(0)
  val listOfAllColumnHistories = collection.mutable.TreeMap[String,ColumnHistoryEncoded]()
  new File(inputDir).listFiles().foreach { f =>
    println(s"Reading ${f.getName}")
    val histories = ColumnHistoryEncoded.fromJsonObjectPerLineFile(f.getAbsolutePath)
    histories.foreach(h => listOfAllColumnHistories.put(h.id,h))
  }
  println(s"Size: ${listOfAllColumnHistories.size}")
  println("Enter anything to terminate")
  val res = io.StdIn.readLine()
  println(s"Entered $res")

}
