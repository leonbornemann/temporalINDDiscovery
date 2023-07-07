package de.hpi.temporal_ind.discovery

import com.typesafe.scalalogging.StrictLogging

case class CommandLineParser(args: Array[String]) extends StrictLogging{
  def printParams() = {
      println("Extracted the following parameter settings:")
      argMap.toIndexedSeq.sortBy(_._1).foreach{case (k,v) => println(s"$k : $v")}
  }

  val argMap = collection.mutable.HashMap[String,String]()
  //default settings
  argMap("--reverse")="false"
  var i = 0
  while(i!=args.size){
    assert(args(i).startsWith("--"))
    if(i==args.size-1 || args(i+1).startsWith("--")) {
      argMap.put(args(i),"true")
      i+=1
    } else {
      argMap.put(args(i),args(i+1))
      i+=2
    }

  }


}
