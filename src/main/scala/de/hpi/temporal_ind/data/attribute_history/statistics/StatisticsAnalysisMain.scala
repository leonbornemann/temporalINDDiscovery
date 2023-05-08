package de.hpi.temporal_ind.data.attribute_history.statistics

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
    if(s(11)=="NaN")
      None
    else
      Some(s(11).toDouble.toInt)
  }

  def getDurationInDays(s: Array[String]) = {
    if(s(6)=="NaN")
      None
    else
      Some(s(6).toInt)
  }

  var count = 0
  var countFiltered = 0
  val frequencyMap = collection.mutable.HashMap[Int,Int]()

  def getnVersionsWithChanges(s: Array[String]) = {
    if(s(5)=="NaN")
      None
    else
      Some(s(5).toDouble.toInt)
  }

  it
    .map(s => {
      count+=1
      s.split(",")
    })
    .withFilter(s => getMedianSize(s).getOrElse(0)>=2 && getDurationInDays(s).getOrElse(0)>=30)
    .withFilter(s => getnVersionsWithChanges(s).isDefined)
    .foreach(s => {
      countFiltered+=1
      val nChangeVersions = getnVersionsWithChanges(s)
      frequencyMap(nChangeVersions.get) = frequencyMap.getOrElse(nChangeVersions.get,0)+1
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
