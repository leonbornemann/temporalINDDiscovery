package de.hpi.temporal_ind.discovery
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input, Output}
import de.hpi.temporal_ind.data.column.data.AbstractColumnVersion
import de.hpi.temporal_ind.data.column.data.original.{KryoSerializableColumnHistory, OrderedColumnHistory}
import de.hpi.temporal_ind.data.ind.variant4.TimeUtil

import java.io.{File, FileInputStream, FileOutputStream}
import java.time.Instant
import scala.jdk.CollectionConverters.ListHasAsScala

class InputDataManager(binaryFile: String,jsonSourceDirs:Option[IndexedSeq[File]]=None) {

  def loadData() = {
    if (jsonSourceDirs.isDefined) {
      val (histories, timeLoadingJson) = TimeUtil.executionTimeInMS(loadDataFromJson())
      println(s"Data Loading Json,$timeLoadingJson")
      histories
    } else {
      val (histories, timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(binaryFile))
      val res = histories.toIndexedSeq
      println(s"Data Loading Binary,$timeLoadingBinary")
      res
    }
  }

  val kryo = new Kryo();
  kryo.setRegistrationRequired(false)
  kryo.setReferences(true)

  def testBinaryLoading(histories: IndexedSeq[OrderedColumnHistory]) = {
    serializeAsBinary(histories, binaryFile)
    val (historiesFromBinary, timeLoadingBinary) = TimeUtil.executionTimeInMS(loadAsBinary(binaryFile))
    println(s"Data Loading Binary,$timeLoadingBinary")
    var curElem = 0
    println("to Read",histories.size)
    historiesFromBinary.foreach { case (h1) => {
      //println(curElem)
      val h2 = histories(curElem)
      if (!((h1.tableId == h2.tableId)
        && (h1.pageID == h2.pageID)
        && (h1.pageTitle == h2.pageTitle)
        && (h1.history.versions.isInstanceOf[collection.SortedMap[Instant, AbstractColumnVersion[String]]])
        && (h1.history.versions == h2.history.versions))) {
        println(s"${h1.id},${h2.id}")
        println(s"${h1.pageID},${h2.pageID}")
        println(s"${h1.pageTitle},${h2.pageTitle}")
        println(h1.history.versions.getClass)
        println(h2.history.versions.getClass)
        println(s"${h1.history.versions}")
        println(h2.history.versions)
        println(s"${h1.id},${h2.id}")
        h1.history.versions.toIndexedSeq.zip(h2.history.versions.toIndexedSeq).foreach{case (t1,t2) => {
          if(t1!=t2){
            println()
          }
        }}
        assert(false)
      }
      curElem+=1
    }
    }
    assert(histories.size == curElem)
    println("Check successful, binary file intact")
  }

  def loadAsBinary(path: String):Iterator[OrderedColumnHistory] = {
    val is = new Input(new FileInputStream(path))
    val it = new Iterator[OrderedColumnHistory]() {


      var curElem = kryo.readClassAndObject(is)

      override def hasNext: Boolean = curElem!=null

      override def next(): OrderedColumnHistory = {
        val cur = curElem
        try{
          curElem=kryo.readClassAndObject(is)
        } catch {
          case e:KryoException => curElem=null
          case e:Throwable => throw(e)
        }
        if(!hasNext){
          is.close()
        }
        OrderedColumnHistory.fromKryoSerializableColumnHistory(cur.asInstanceOf[KryoSerializableColumnHistory])
      }
    }
    it
  }

  def serializeAsBinary(histories: IndexedSeq[OrderedColumnHistory], path: String) = {
    val os = new Output(new FileOutputStream(new File(path)))
    histories
      .map(och => och.toKryoSerializableColumnHistory)
      .foreach(k => kryo.writeClassAndObject(os,k))
//    val list = new java.util.ArrayList[KryoSerializableColumnHistory]()
//    historySerializable.foreach(list.add(_))
//    kryo.writeClassAndObject(os, list)
    //os.write(kryo.toBytesWithClass(histories.toBuffer))//.writeClassAndObject(os,histories.toBuffer)
    kryo.writeClassAndObject(os,histories.last)
    os.close()
  }

  private def loadDataFromJson() = {
    val (histories, timeDataLoading) = TimeUtil.executionTimeInMS(loadJsonHistories())
    histories
  }

  def loadJsonHistories() =
    jsonSourceDirs.get
      .flatMap(sourceDir => OrderedColumnHistory
        .readFromFiles(sourceDir)
        .toIndexedSeq)

}
