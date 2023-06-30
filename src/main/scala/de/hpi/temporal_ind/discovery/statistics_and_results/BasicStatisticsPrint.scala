package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.GLOBAL_CONFIG
import de.hpi.temporal_ind.discovery.input_data.InputDataManager
import de.hpi.temporal_ind.util.Util

import java.time.temporal.ChronoUnit

object BasicStatisticsPrint extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val dataSourceFile = args(0)
  val dm = new InputDataManager(dataSourceFile)
  val data = dm.loadData()
  var count = 0
  val stats = data.map(och => {
    count+=1
    if (count % 100000 == 0)
      println("processed", count)
    val seq:IndexedSeq[Int] = och.history.versions.values.map(t => t.values.size).toIndexedSeq
    Stats(och.history.versions.size,
      och.nonEmptyIntervals.intervals.map(i => ChronoUnit.DAYS.between(i._1, i._2)).sum,
      seq)
  })
  //print avg and std:
  printMeanAndSTDInt(stats.map(_.nVersions))
  printMeanAndSTDInt(stats.flatMap(_.sizes))
  printMeanAndSTDLong(stats.map(_.lifeTimeDays))
  print("min change count",stats.map(_.nVersions).min-1)
  print("min lifetime days",stats.map(_.lifeTimeDays).min)


  case class Stats(nVersions: Int, lifeTimeDays:Long, sizes:IndexedSeq[Int])

  def printMeanAndSTDInt(value: IndexedSeq[Int]) = {
    val sum = value.sum
    val mean = sum / value.size.toDouble
    val std = Math.sqrt(value.map(v => Math.pow(v - mean, 2)).sum / value.size)
    val median = value.sorted.apply(value.size/2)
    println(median,mean, std)
  }

  def printMeanAndSTDLong(value: IndexedSeq[Long]) = {
    val sum = value.sum
    val mean = sum / value.size.toDouble
    val std = Math.sqrt(value.map(v => Math.pow(v - mean, 2)).sum)
    val median = value.sorted.apply(value.size/2)
    println(median,mean, std)
  }
}
