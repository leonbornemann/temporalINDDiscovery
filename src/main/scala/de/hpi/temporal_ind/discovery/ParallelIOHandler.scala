package de.hpi.temporal_ind.discovery

import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.statistics_and_results.{ResultSerializer, StandardResultSerializer}

import java.io.{File, PrintWriter}

class ParallelIOHandler(rootDir:File,
                        queryFile:File,
                        bloomFilterSize: Int,
                        timeSliceChoiceMethod: TimeSliceChoiceMethod.Value,
                        seed: Long) {

  val availableResultSerializers = scala.collection.mutable.ListBuffer[StandardResultSerializer]()
  var outputDirCounter = 0
  def getOrCreateNEwResultSerializer() = {
    availableResultSerializers.synchronized {
      if(availableResultSerializers.isEmpty){
        val newDir = new File(rootDir.getAbsolutePath)
        println(s"Creating $newDir")
        newDir.mkdirs()
        val serializer = new StandardResultSerializer(newDir,queryFile,bloomFilterSize,timeSliceChoiceMethod, seed,Some(outputDirCounter))
        outputDirCounter += 1
        serializer
      } else {
        availableResultSerializers.remove(0)
      }
    }
  }

  def releaseResultSerializer(resultSerializer: StandardResultSerializer) = {
    availableResultSerializers.synchronized{
      availableResultSerializers += resultSerializer
    }
  }
}
