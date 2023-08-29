package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.discovery.indexing.time_slice_choice.{DynamicWeightedRandomTimeSliceChooser, TimeSliceChooser, WeightedShuffledTimestamps}
import de.hpi.temporal_ind.discovery.indexing.{BloomfilterIndex, MultiLevelIndexStructure, MultiTimeSliceIndexStructure, TimeSliceChoiceMethod}
import de.hpi.temporal_ind.discovery.input_data.{ColumnHistoryStorage, EnrichedColumnHistory}
import de.hpi.temporal_ind.util.TimeUtil

import java.io.File
import java.time.Instant
import scala.concurrent.Await
import scala.util.Random

class TINDIndexBuilder(metaDir:File,
                       seed:Long,
                       timeSliceChoiceMethod: TimeSliceChoiceMethod.Value,
                       expectedQueryParameters:TINDParameters,
                       random:Random,
                       reverseSearch:Boolean,
                       bloomfilterSize:Int) extends StrictLogging{

  def buildMultiIndexStructure(historiesEnriched: ColumnHistoryStorage, numTimeSliceIndices: Int) = {
    val (indexEntireValueset, requiredValuesIndexBuildTime) = TimeUtil.executionTimeInMS(getRequiredValuesetIndex(historiesEnriched))
    val (timeSliceIndices, timeSliceIndexBuildTimes) = buildTimeSliceIndices(historiesEnriched, numTimeSliceIndices)
    val timeSliceIndexStructure = new MultiTimeSliceIndexStructure(timeSliceIndices, timeSliceIndexBuildTimes)
    new MultiLevelIndexStructure(Some(indexEntireValueset), timeSliceIndexStructure, requiredValuesIndexBuildTime)
  }

  def buildTimeSliceIndices(historiesEnriched: ColumnHistoryStorage, indicesToBuild: Int) = {
    val weightedShuffleFile = WeightedShuffledTimestamps.getImportFile(metaDir, seed, timeSliceChoiceMethod)
    val timeSliceChooser = TimeSliceChooser.getChooser(timeSliceChoiceMethod, historiesEnriched, expectedQueryParameters, random, weightedShuffleFile, reverseSearch)

    val slices = collection.mutable.ArrayBuffer[(Instant, Instant)]()
    (0 until indicesToBuild).foreach(_ => {
      val timeSlice = timeSliceChooser.getNextTimeSlice()
      slices += timeSlice
    })
    if (!weightedShuffleFile.exists() && timeSliceChoiceMethod == TimeSliceChoiceMethod.DYNAMIC_WEIGHTED_RANDOM) {
      timeSliceChooser.asInstanceOf[DynamicWeightedRandomTimeSliceChooser].exportAsFile(weightedShuffleFile)
    }
    logger.debug(s"Running Index Build for ${historiesEnriched.histories.size} attributes with time slices: $slices")
    var buildTimes = collection.mutable.ArrayBuffer[Double]()
    //concurrent index building to speed up index construction:
    val handler = new ParallelExecutionHandler(slices.size)
    val futures = slices.map { case (begin, end) => {
      handler.addAsFuture({
        val (timeSliceIndex, timeSliceIndexBuild) = TimeUtil.executionTimeInMS(getIndexForTimeSlice(historiesEnriched, begin, end))
        buildTimes += timeSliceIndexBuild
        ((begin, end), timeSliceIndex)
      })
    }
    }
    handler.awaitTermination()
    val results = futures.map(f => Await.result(f, scala.concurrent.duration.Duration.MinusInf))
    val indexMap = collection.mutable.TreeMap[(Instant, Instant), BloomfilterIndex]() ++ results
    (indexMap, buildTimes)
  }

  def getRequiredValuesetIndex(historiesEnriched: ColumnHistoryStorage) = {
    val func = if (!reverseSearch) (e: EnrichedColumnHistory) => e.allValues else (e: EnrichedColumnHistory) => e.requiredValues(expectedQueryParameters)
    new BloomfilterIndex(historiesEnriched.histories,
      bloomfilterSize,
      func
    )
  }

  def getIndexForTimeSlice(historiesEnriched: ColumnHistoryStorage, lower: Instant, upper: Instant, size: Int = bloomfilterSize) = {
    logger.debug(s"Building Index for [$lower,$upper)")
    val (beginDelta, endDelta) = (lower.minusNanos(expectedQueryParameters.absDeltaInNanos), upper.plusNanos(expectedQueryParameters.absDeltaInNanos))
    new BloomfilterIndex(historiesEnriched.histories,
      size,
      (e: EnrichedColumnHistory) => e.valueSetInWindow(beginDelta, endDelta)
    )
  }

}
