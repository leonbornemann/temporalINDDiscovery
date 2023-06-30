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
  val stats = data.map(och => {
    val seq:IndexedSeq[Int] = och.history.versions.values.map(t => t.values.size).toIndexedSeq
    Stats(och.history.versions.size,
      och.nonEmptyIntervals.intervals.map(i => ChronoUnit.DAYS.between(i._1, i._2)).sum,
      seq)
  })
  //print avg and std:
  printMeanAndSTDInt(stats.map(_.nVersions))
  printMeanAndSTDInt(stats.flatMap(_.sizes))
  printMeanAndSTDLong(stats.map(_.lifeTimeNanos))


  case class Stats(nVersions: Int,lifeTimeNanos:Long,sizes:IndexedSeq[Int])

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
