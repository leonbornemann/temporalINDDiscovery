package de.hpi.temporal_ind.discovery.indexing

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, ValuesInTimeWindow}
import de.hpi.temporal_ind.discovery.TINDParameters
import de.hpi.temporal_ind.util.TimeUtil
import de.metanome.algorithms.many.bitvectors.BitVector
import de.metanome.algorithms.many.{Column, INDDetectionWorkerQuery, MANY}

import java.time.Instant
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/***
 *
 * @param input
 * @param generateValueSetToIndex
 * @param generateQueryValueSets This can be multiple value sets because for time-slice indices we need separately check all versions of the query in the time period and or the result candidates
 */
class BloomfilterIndex(input: IndexedSeq[EnrichedColumnHistory],
                       bloomfilterSize:Int,
                       generateValueSetToIndex:(EnrichedColumnHistory => collection.Set[String])) extends StrictLogging{



  val outFile = "/home/leon/data/temporalINDDiscovery/wikipedia/discovery/testOutput/test.txt"
  val many = new MANY()

  def getColumnsAsJavaList(): util.List[Column] = {
    val list = (collection.mutable.ArrayBuffer() ++ input)
      .map{case och =>
        val col = new Column(och.colID,generateValueSetToIndex(och).toSeq.asJava)
        col.setTableName(och.tableID)
        col
      }
      .asJava
    list
  }

  def setStandardParameters() = {
    many.setDirectColumnInput(getColumnsAsJavaList())
    //anelosimus.setRelationalInputConfigurationValue(MANY.Identifier.RELATIONAL_INPUT.name(), fileInputGenerators);//anelosimus.setRelationalInputConfigurationValue(MANY.Identifier.RELATIONAL_INPUT.name(), fileInputGenerators);
    val resultReceiver = new INDResultCounter()
    many.setResultReceiver(resultReceiver)
    many.setIntegerConfigurationValue(MANY.Identifier.INPUT_ROW_LIMIT.name, Integer.valueOf(-1))
    many.setIntegerConfigurationValue(MANY.Identifier.HASH_FUNCTION_COUNT.name, Integer.valueOf(3))
    many.setIntegerConfigurationValue(MANY.Identifier.BIT_VECTOR_SIZE.name, Integer.valueOf(bloomfilterSize))
    many.setIntegerConfigurationValue(MANY.Identifier.DEGREE_OF_PARALLELISM.name, Integer.valueOf(1))
    many.setIntegerConfigurationValue(MANY.Identifier.PASSES.name, Integer.valueOf(1))
    many.setBooleanConfigurationValue(MANY.Identifier.VERIFY.name, java.lang.Boolean.valueOf(true))
    many.setBooleanConfigurationValue(MANY.Identifier.OUTPUT.name, java.lang.Boolean.valueOf(true))
    many.setBooleanConfigurationValue(MANY.Identifier.FILTER_NON_UNIQUE_REFS.name, java.lang.Boolean.valueOf(false))
    many.setIntegerConfigurationValue(MANY.Identifier.REF_COVERAGE_MIN_PERCENTAGE.name, Integer.valueOf(0))
    many.setBooleanConfigurationValue(MANY.Identifier.FILTER_NULL_COLS.name, java.lang.Boolean.valueOf(false))
    many.setBooleanConfigurationValue(MANY.Identifier.FILTER_NUMERIC_AND_SHORT_COLS.name, java.lang.Boolean.valueOf(false))
    many.setBooleanConfigurationValue(MANY.Identifier.FILTER_DEPENDENT_REFS.name, java.lang.Boolean.valueOf(false))
    many.setBooleanConfigurationValue(MANY.Identifier.FASTVECTOR.name, java.lang.Boolean.valueOf(false))
    many.setBooleanConfigurationValue(MANY.Identifier.CONDENSE_MATRIX.name, java.lang.Boolean.valueOf(false))
    many.setBooleanConfigurationValue(MANY.Identifier.STRATEGY_REF2DEPS.name, java.lang.Boolean.valueOf(false))
  }
  setStandardParameters()
  val (_,time) = TimeUtil.executionTimeInMS(many.buildMatrix())
  TimeUtil.logRuntime(time,"ms","Bit Matrix Creation For Value Sets")

  def bitVectorToColumns(res: BitVector[_]) = {
    var curColumnIndex=res.next(-1)
    val columns = collection.mutable.ArrayBuffer[EnrichedColumnHistory]()
    while(curColumnIndex!= -1){
      columns += input(curColumnIndex)
      curColumnIndex = res.next(curColumnIndex)
    }
    columns
  }

  def iterateThroughSetBitsOfBitVector(bv:BitVector[_]):Iterator[Int] = {
    new Iterator[Int]() {

      var cur = bv.next(-1)

      override def hasNext: Boolean = cur!= -1

      override def next(): Int = {
        val toReturn = cur
        cur = bv.next(cur)
        toReturn
      }
    }
  }

  def validateContainmentOfSets(queryValueSets: Seq[ValuesInTimeWindow],
                                queryParameters: TINDParameters,
                                res: BitVector[_],
                                candidateToViolationMap:Option[collection.mutable.HashMap[Int,Double]]=None) = {
    var curColumnIndex = res.next(0)
    val toSetTo0 = collection.mutable.ArrayBuffer[Int]()
    if(candidateToViolationMap.isEmpty){
      assert(queryValueSets.size==1)
    }
    while (curColumnIndex != -1) {
      val curCol = input(curColumnIndex)
      queryValueSets.foreach(q => {
        if(!q.values.subsetOf(generateValueSetToIndex(curCol))){
          if(candidateToViolationMap.isEmpty){
            //we have an immediate violation because this is a required values bloomfilter
            toSetTo0 += curColumnIndex
          } else {
            //we track violations
            val newViolation = candidateToViolationMap.get.getOrElse(curColumnIndex, 0.0) + queryParameters.omega.weight(q.beginInclusive, q.endExclusive)
            candidateToViolationMap.get(curColumnIndex) = newViolation
            if (newViolation > queryParameters.absoluteEpsilon) {
              toSetTo0 += curColumnIndex
            }
          }
        }
      })
      curColumnIndex = res.next(curColumnIndex)
    }
    toSetTo0.foreach(i => res.clear(i))
  }

  def validateContainmentOfSetsReverseQueryRequiredValues(queryValueSet: ValuesInTimeWindow,
                                                          queryParameters: TINDParameters,
                                                          res: BitVector[_]) = {
    var curColumnIndex = res.next(0)
    val toSetTo0 = collection.mutable.ArrayBuffer[Int]()
    while (curColumnIndex != -1) {
      val curCol = input(curColumnIndex)
      val indexSet = curCol.requiredValues(queryParameters)
      if(!indexSet.subsetOf(queryValueSet.values)){
        toSetTo0 += curColumnIndex
      }
      curColumnIndex = res.next(curColumnIndex)
    }
    toSetTo0.foreach(i => res.clear(i))
  }

  def validateContainmentOfSetsReverseSearchTimeSliceIndex(queryValueSet: ValuesInTimeWindow,
                                                           queryParameters: TINDParameters,
                                                           res: BitVector[_],
                                                           candidateToViolationMap: Some[mutable.HashMap[Int, Double]]) = {
    var curColumnIndex = res.next(0)
    val toSetTo0 = collection.mutable.ArrayBuffer[Int]()
    while (curColumnIndex != -1) {
      val curCol = input(curColumnIndex)
      val indexSets = curCol.getValueSetsInWindow(queryValueSet.beginInclusive,queryValueSet.endExclusive)
        .iterator
      while(indexSets.hasNext && candidateToViolationMap.get.getOrElse(curColumnIndex, 0.0) <=queryParameters.absoluteEpsilon) {
        val indexedSet = indexSets.next()
        if(!indexedSet.values.subsetOf(queryValueSet.values)){
          val newViolation = candidateToViolationMap.get.getOrElse(curColumnIndex, 0.0) + queryParameters.omega.weight(indexedSet.beginInclusive, indexedSet.endExclusive)
          candidateToViolationMap.get(curColumnIndex) = newViolation
        }
      }
      if(candidateToViolationMap.get.getOrElse(curColumnIndex, 0.0) > queryParameters.absoluteEpsilon){
        toSetTo0 += curColumnIndex
      }
      curColumnIndex = res.next(curColumnIndex)
    }
    toSetTo0.foreach(i => res.clear(i))
  }

  private def noEarlyAbortIndexQuery(queryValueSet: ValuesInTimeWindow,
                                     preFilteredCandidates:Option[BitVector[_]] = None,
                                     previousResult:Option[BitVector[_]]) = {
    var resNew = executeQuery(preFilteredCandidates, queryValueSet, false)
    if (previousResult.isEmpty) {
      resNew
    } else {
      resNew = previousResult.get.or(resNew) // we take the or here because if any of the versions within a time slice is contained, we cannot prune the candidate
      if (preFilteredCandidates.isDefined)
        resNew = resNew.and(preFilteredCandidates.get) //doing the and again is probably not necessary (?)
      resNew
    }
  }

  def queryWithBitVectorResult(queryValueSets:Seq[ValuesInTimeWindow],
                               queryParameters:TINDParameters,
                               preFilteredCandidates:Option[BitVector[_]] = None,
                               candidateToViolationMap:Option[collection.mutable.HashMap[Int,Double]]=None) = {
    var res:Option[BitVector[_]] = None

    val (_,queryTime) = TimeUtil.executionTimeInMS({
      if(queryValueSets.size==1 && queryParameters.omega.weight(queryValueSets.head.beginInclusive,queryValueSets.head.endExclusive)>queryParameters.absoluteEpsilon){
        // we know that we have the exact pruning power we need
        val queryValueSet = queryValueSets.head
        res = Some(noEarlyAbortIndexQuery(queryValueSet,preFilteredCandidates,res))
      } else {
        res = Some(preFilteredCandidates.get.copy())
        queryValueSets.foreach { queryValueSet =>
          val queryWeight = queryParameters.omega.weight(queryValueSet.beginInclusive, queryValueSet.endExclusive)
          val resNew = executeQuery(preFilteredCandidates, queryValueSet,false)
          val toTrack = res.get.copy().and(resNew.copy().flip())
          val it = iterateThroughSetBitsOfBitVector(toTrack)
          it.foreach(curBit => {
            val prev = candidateToViolationMap.get.getOrElse(curBit,0.0)
            if(prev+queryWeight<=queryParameters.absoluteEpsilon){
              //add new violation score
              candidateToViolationMap.get(curBit) = prev+queryWeight
            } else {
              //we can prune,, remove the bit from the map and clear the bit in the in the original result, so it will never show up again
              candidateToViolationMap.get.remove(curBit)
              res.get.clear(curBit)
            }
          })
          res.get.and(toTrack.or(resNew)) //updates initial candidates
        }
      }
    })
    if(res.isEmpty)
      println()
    (res.get,queryTime,0.0)
  }

  def getReverseQueryViolationWeight(curBit: Int, beginInclusive: Instant, endExclusive: Instant, queryParameters: TINDParameters) = {
    val column = input(curBit)
    val versions = column.getValueSetsInWindow(beginInclusive,endExclusive)
    versions.map(v => queryParameters.omega.weight(v.beginInclusive,v.endExclusive)).min
  }

  def queryWithBitVectorResultReverseSearch(queryValueSet: ValuesInTimeWindow,
                                            queryParameters: TINDParameters,
                                            preFilteredCandidates: Option[BitVector[_]] = None,
                                            candidateToViolationMap: Option[collection.mutable.HashMap[Int, Double]] = None) = {
    var res: Option[BitVector[_]] = None
    val (_, queryTime) = TimeUtil.executionTimeInMS({
      res = Some(preFilteredCandidates.get.copy())
      val resNew = executeQuery(preFilteredCandidates, queryValueSet, true)
      val toTrack = res.get.copy().and(resNew.copy().flip())
      val it = iterateThroughSetBitsOfBitVector(toTrack)
      it.foreach(curBit => {
        val prev = candidateToViolationMap.get.getOrElse(curBit, 0.0)
        val minViolation = getReverseQueryViolationWeight(curBit,queryValueSet.beginInclusive,queryValueSet.endExclusive,queryParameters)
        if (prev + minViolation <= queryParameters.absoluteEpsilon) {
          //add new violation score
          candidateToViolationMap.get(curBit) = prev + minViolation
        } else {
          //we can prune,, remove the bit from the map and clear the bit in the in the original result, so it will never show up again
          candidateToViolationMap.get.remove(curBit)
          res.get.clear(curBit)
        }
      })
    })
    if (res.isEmpty)
      println()
    (res.get, queryTime, 0.0)
  }

  def queryWithBitVectorResultReverseSearchRequiredValues(queryValueSets: IndexedSeq[ValuesInTimeWindow],
                                            queryParameters: TINDParameters) = {
    var res: BitVector[_] = null
    val (_, queryTime) = TimeUtil.executionTimeInMS({
      assert(queryValueSets.size==1)
      res = executeQuery(None, queryValueSets.head, true)
    })
    (res, queryTime, 0.0)
  }

  private def executeQuery(preFilteredCandidates: Option[BitVector[_]], queryValueSet: ValuesInTimeWindow, reverseSearch: Boolean): BitVector[_] = {
    val querySig = many.applyBloomfilter(queryValueSet.values.asJava)
    val worker = new INDDetectionWorkerQuery(many, querySig, 0)
    val resNew = if (preFilteredCandidates.isDefined)
      worker.executeQuery(preFilteredCandidates.get,reverseSearch) //
    else {
      worker.executeQuery(reverseSearch)
      //TODO - implement in MANY
    }
    resNew
  }


//  TimeUtil.logRuntime(timeWorker,"ms","Single Worker Execution For Value Set containment")
//  val results = worker.getColumnToResultBitVector.asScala
//  val pr = new PrintWriter(s"$outFile.csv")
//  pr.println("Estimated Containment")
//  results.foreach{case (i,bv) => {
//    val estimated = (0 until bv.size()).map(j => if (bv.get(j)) 1 else 0).sum
//    pr.println(estimated)
//  }}
//  pr.close()
//  assert(false)
}

