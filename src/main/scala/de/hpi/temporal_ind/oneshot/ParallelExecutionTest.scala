package de.hpi.temporal_ind.oneshot

import com.typesafe.scalalogging.StrictLogging
import de.hpi.temporal_ind.discovery.indexing.TimeSliceChoiceMethod
import de.hpi.temporal_ind.discovery.{ParallelIOHandler, ParallelQuerySearchHandler}

import java.io.File

object ParallelExecutionTest extends App with StrictLogging{
  logger.debug("begin")
  val totalNFutures = 10
  ParallelQuerySearchHandler.initContext(2)
  println(List(1,2,3).grouped(2))
  val parallelExecutor = new ParallelQuerySearchHandler(totalNFutures)
  val handler = new ParallelIOHandler(new File("/home/leon/test/"),new File("test"),1024,TimeSliceChoiceMethod.WEIGHTED_RANDOM,13)
  for(i <- 0 until totalNFutures){
    parallelExecutor.addAsFuture({
      println(s"Beginning $i")
      val serializer = handler.getOrCreateNEwResultSerializer()
      serializer.totalResultsStats.println(i)
      Thread.sleep(i*1000)
      println(s"Terminating $i")
      handler.releaseResultSerializer(serializer)
    })
  }
  println("Awaiting termination")
  parallelExecutor.awaitTermination
  println("Termination completed")
  handler.availableResultSerializers.foreach(_.closeAll())
  logger.debug("end")

}
