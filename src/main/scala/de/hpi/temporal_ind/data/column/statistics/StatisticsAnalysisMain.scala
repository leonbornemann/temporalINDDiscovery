package de.hpi.temporal_ind.data.column.statistics

import java.io.PrintWriter
import scala.io.Source

object StatisticsAnalysisMain extends App {
  val statFile = args(0)
  val outFile = args(1)
  val it = Source.fromFile(statFile)
    .getLines()
  //ignore header:
  it.next()

  def getMedianSize(s: Array[String]) = {
    s(11).toInt
  }

  def getDurationInDays(s: Array[String]) = {
    s(6).toInt
  }

  var count = 0
  var countFiltered = 0
  val frequencyMap = collection.mutable.HashMap[Int,Int]()

  def getnVersionsWithChanges(s: Array[String]) = {
    s(5).toInt
  }

  it
    .map(s => {
      count+=1
      s.split(",")
    })
    .withFilter(s => getMedianSize(s)>=2 && getDurationInDays(s)>=30)
    .foreach(s => {
      countFiltered+=1
      val nChangeVersions = getnVersionsWithChanges(s)
      frequencyMap(nChangeVersions) = frequencyMap.getOrElse(nChangeVersions,0)+1
    })
  val pr = new PrintWriter(outFile)
  frequencyMap
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(t => {
      val csvStr = t._1 + "," + t._2
      println(csvStr)
      pr.println(csvStr)
    })
  pr.close()
}
