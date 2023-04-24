package de.hpi.temporal_ind.discovery.indexing

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil
import de.hpi.temporal_ind.discovery.input_data.{EnrichedColumnHistory, ValuesInTimeWindow}
import de.hpi.temporal_ind.discovery.INDResultCounter
import de.metanome.algorithms.many.bitvectors.BitVector
import de.metanome.algorithms.many.{Column, INDDetectionWorkerQuery, MANY}

import java.util
import scala.collection.JavaConverters._

/***
 *
 * @param input
 * @param generateValueSetToIndex
 * @param generateQueryValueSets This can be multiple value sets because for time-slice indices we need separately check all versions of the query in the time period and or the result candidates
 */
class BloomfilterIndex(input: IndexedSeq[EnrichedColumnHistory],
                       bloomfilterSize:Int,
                       generateValueSetToIndex:(EnrichedColumnHistory => collection.Set[String]),
                       generateQueryValueSets:(EnrichedColumnHistory => Seq[ValuesInTimeWindow]),
                       absoluteEpsilonNanos:Long) extends StrictLogging{
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
    var curColumnIndex=res.next(0)
    val columns = collection.mutable.ArrayBuffer[EnrichedColumnHistory]()
    while(curColumnIndex!= -1){
      columns += input(curColumnIndex)
      curColumnIndex = res.next(curColumnIndex)
    }
    columns
  }

  def validateContainment(query:EnrichedColumnHistory,res:BitVector[_]) = {
    validateContainmentOfSets(generateQueryValueSets(query),res)
  }

  def iterateThroughSetBitsOfBitVector(bv:BitVector[_]):Iterator[Int] = {
    new Iterator[Int]() {

      var cur = bv.next(0)

      override def hasNext: Boolean = cur!= -1

      override def next(): Int = {
        val toReturn = cur
        cur = bv.next(cur)
        toReturn
      }
    }
  }

  def validateContainmentOfSets(queryValueSets: Seq[ValuesInTimeWindow],
                          res: BitVector[_]) = {
    var curColumnIndex = res.next(0)
    val toSetTo0 = collection.mutable.ArrayBuffer[Int]()
    while (curColumnIndex != -1) {
      val curCol = input(curColumnIndex)
      if(!queryValueSets.exists(_.values.subsetOf(generateValueSetToIndex(curCol)))){
        toSetTo0 += curColumnIndex
      }
      curColumnIndex = res.next(curColumnIndex)
    }
    toSetTo0.foreach(i => res.clear(i))
  }

  private def noEarlyAbortIndexQuery(queryValueSet: ValuesInTimeWindow,
                                     preFilteredCandidates:Option[BitVector[_]] = None,
                                     previousResult:Option[BitVector[_]]) = {
    var resNew = executeQuery(preFilteredCandidates, queryValueSet)
    if (previousResult.isEmpty) {
      resNew
    } else {
      resNew = previousResult.get.or(resNew) // we take the or here because if any of the versions within a time slice is contained, we cannot prune the candidate
      if (preFilteredCandidates.isDefined)
        resNew = resNew.and(preFilteredCandidates.get) //doing the and again is probably not necessary (?)
      resNew
    }
  }

  def queryWithBitVectorResult(q:EnrichedColumnHistory,
                               preFilteredCandidates:Option[BitVector[_]] = None,
                               validate:Boolean=true,
                               candidateToViolationMap:Option[collection.mutable.HashMap[Int,Long]]=None) = {
    var res:Option[BitVector[_]] = None

    val (_,queryTime) = TimeUtil.executionTimeInMS({
      val queryValueSets = generateQueryValueSets(q)
      if(queryValueSets.size==1){
        // we know that we have the exact pruning power we need
        val queryValueSet = queryValueSets.head
        res = Some(noEarlyAbortIndexQuery(queryValueSet,preFilteredCandidates,res))
      } else if(candidateToViolationMap.isEmpty) {
        queryValueSets.foreach { queryValueSet =>
          res = Some(noEarlyAbortIndexQuery(queryValueSet, preFilteredCandidates, res))
        }
        res
      } else {
        res = Some(preFilteredCandidates.get.copy())
        queryValueSets.foreach { queryValueSet =>
          val queryWeightInNanos = TimeUtil.durationNanos(queryValueSet.beginInclusive, queryValueSet.endExclusive)
          val resNew = executeQuery(preFilteredCandidates, queryValueSet)
          val toTrack = res.get.copy().and(resNew.copy().flip())
          val it = iterateThroughSetBitsOfBitVector(toTrack)
          it.foreach(curBit => {
            val prev = candidateToViolationMap.get.getOrElse(curBit,0L)
            if(prev+queryWeightInNanos<absoluteEpsilonNanos){
              //add new violation score
              candidateToViolationMap.get(curBit) = prev+queryWeightInNanos
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
    val validationTime = if(validate){
      TimeUtil.executionTimeInMS(validateContainment(q,res.get))._2
    } else {
      0.0
    }
    if(res.isEmpty)
      println()
    (res.get,queryTime,validationTime)
  }

  private def executeQuery(preFilteredCandidates: Option[BitVector[_]], queryValueSet: ValuesInTimeWindow) = {
    val querySig = many.applyBloomfilter(queryValueSet.values.asJava)
    val worker = new INDDetectionWorkerQuery(many, querySig, 0)
    val resNew = if (preFilteredCandidates.isDefined)
      worker.executeQuery(preFilteredCandidates.get)
    else {
      worker.executeQuery()
    }
    resNew
  }

  def query(q:EnrichedColumnHistory) = {
    val res = queryWithBitVectorResult(q)
    val candidates = bitVectorToColumns(res._1)
    candidates
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