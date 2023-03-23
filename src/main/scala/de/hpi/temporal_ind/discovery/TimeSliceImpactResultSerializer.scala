package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.data.ind.ShifteddRelaxedCustomFunctionTemporalIND

import java.io.File
import scala.collection.mutable.ArrayBuffer

class TimeSliceImpactResultSerializer(targetDir: File) extends ResultSerializer {


  override def addTrueTemporalINDs(trueTemporalINDs: ArrayBuffer[ShifteddRelaxedCustomFunctionTemporalIND[String]]): Unit = ???

  override def addIndividualResultStats(individualStatLine: IndividualResultStats): Unit = ???

  override def addBasicQueryInfoRow(validationStatRow: BasicQueryInfoRow): Unit = ???

  override def closeAll(): Unit = ???

  override def addTotalResultStats(totalResultSTatsLine: TotalResultStats): Unit = ???
}
