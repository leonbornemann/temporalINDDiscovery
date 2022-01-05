package de.hpi.temporal_ind.data.ind

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.column.data.encoded.ColumnHistoryEncoded
import de.hpi.temporal_ind.data.column.data.original.ColumnHistory
import de.hpi.temporal_ind.data.column.io.ColumnHistoryBuffer
import de.hpi.temporal_ind.data.column.statistics.TemporalINDStatRow
import de.hpi.temporal_ind.data.ind.variant4.Variant4TemporalIND

import java.io.{File, PrintWriter}

object MANYInputINDValidator extends App with StrictLogging{
  val manyInputFIle = new File(args(0))
  val columnHistoriesByID = ColumnHistoryEncoded.loadIntoMap(new File(args(1)))
  val deltaInDays = args(2).toInt
  val pr = new PrintWriter(args(3))
  //val maxEpsilon = args(4).toInt
//  val ids = Vector("00008b30-68c2-437d-ab2f-b69e49db5be1", "00013074-0ce6-4413-93d9-dd30fcaed09b", "00014168-9042-4ca6-a62f-9f798eb2ecf3", "000442d8-b182-4090-a270-3c50becc289f")
//  implicit class Crossable[X](xs: Traversable[X]) {
//    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
//  }
//  TemporalINDStatRow.appendSchemaToFile(pr)
//  val inds = (ids cross ids).foreach { case (lhsCol, rhsCol) =>
//    val lhs = columnHistoriesByID(lhsCol).asOrderedVersionMap
//    val rhs = columnHistoriesByID(rhsCol).asOrderedVersionMap
//    val strictTemporalIND = new StrictTemporalIND(lhs, rhs)
//    val variant1TemporalIND = new Variant1TemporalIND(lhs,rhs,deltaInDays)
//    val variant3TemporalIND = new Variant3TemporalIND(lhs,rhs,deltaInDays)
//    val statRow = new TemporalINDStatRow(lhs,rhs,strictTemporalIND,variant1TemporalIND,variant3TemporalIND,deltaInDays)
//    statRow.appendToCSVFile(pr)
//    //val variant4 = new Variant4TemporalIND(lhs,rhs,deltaInDays,maxEpsilon)
//  }
  val inds = StaticInclusionDependency.readFromMANYOutputFile(manyInputFIle)
  //val inds = Seq(StaticInclusionDependency.fromManyOutputString("[92428275-10_8140886.csv.f23238b8-82cb-444b-9c43-f0c8c115bf78][=[98091879-1_8730532.csv.01400e3f-0f4e-4bfd-af5e-98774d87bfd9]"))
  var counter = 0
  inds
    .foreach(ind => {
      val lhsCol = ind.lhsColumnID
      val rhsCol = ind.rhsColumnID
      val lhs = columnHistoriesByID(lhsCol).asOrderedVersionMap
      val rhs = columnHistoriesByID(rhsCol).asOrderedVersionMap
      val strictTemporalIND = new StrictTemporalIND(lhs, rhs)
      val variant1TemporalIND = new Variant1TemporalIND(lhs,rhs,deltaInDays)
      val variant3TemporalIND = new Variant3TemporalIND(lhs,rhs,deltaInDays)
      val statRow = new TemporalINDStatRow(lhs,rhs,strictTemporalIND,variant1TemporalIND,variant3TemporalIND,deltaInDays)
      statRow.appendToCSVFile(pr)
      counter += 1
      if(counter %1000000==0)
        logger.debug(s"Processed $counter")

  })
  pr.close()

  //  byPageIDCombination
//    .foreach{case (lhsPageID,rhsPageID) => {
//
//    }}

}
