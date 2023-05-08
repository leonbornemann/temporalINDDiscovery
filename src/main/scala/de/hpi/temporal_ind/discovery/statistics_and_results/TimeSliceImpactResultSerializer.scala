package de.hpi.temporal_ind.discovery.statistics_and_results

import de.hpi.temporal_ind.data.ind.EpsilonOmegaDeltaRelaxedTemporalIND

import java.io.File
import scala.collection.mutable.ArrayBuffer

class TimeSliceImpactResultSerializer(targetDir: File) extends ResultSerializer {


  override def addTrueTemporalINDs(trueTemporalINDs: Iterable[EpsilonOmegaDeltaRelaxedTemporalIND[String]]): Unit = ???

  override def addIndividualResultStats(individualStatLine: IndividualResultStats): Unit = ???

  override def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow): Unit = ???

  override def closeAll(): Unit = ???

  override def addTotalResultStats(totalResultSTatsLine: TotalResultStats): Unit = ???
}
