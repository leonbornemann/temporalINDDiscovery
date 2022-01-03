package de.hpi.temporal_ind.data.column.io

import de.hpi.temporal_ind.data.column.ColumnHistory

import java.io.File
import scala.io.Source

object MemoryConsumptionTest extends App {

  val inputDir = args(0)
  val listOfAllColumnHistories = new File(inputDir).listFiles().map(f =>
    ColumnHistory.fromJsonFile(f.getAbsolutePath)
  )
  println(s"Size: ${listOfAllColumnHistories.size}")
  println("Enter anything to terminate")
  val res = io.StdIn.readLine()
  println(s"Entered $res")

}
