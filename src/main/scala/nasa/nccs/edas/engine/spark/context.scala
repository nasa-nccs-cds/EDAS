package nasa.nccs.edas.engine.spark

import java.nio.file.{Files, Path, Paths}

import nasa.nccs.caching._
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data._
import ucar.nc2.time.CalendarDate
import nasa.nccs.edas.engine.WorkflowNode
import nasa.nccs.edas.kernels.{Kernel, KernelContext}
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{Loggable}
import org.apache.spark.rdd.RDD
import nasa.nccs.edas.utilities
import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkEnv}
import ucar.ma2
import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession
import com.sun.management.OperatingSystemMXBean
import nasa.nccs.cdapi.tensors.CDCoordMap
import nasa.nccs.edas.engine.EDASExecutionManager.logger
import nasa.nccs.edas.rdd.CDRecord
import ucar.nc2.dataset.CoordinateAxis1DTime
import org.apache.log4j.{Level, Logger}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.io.Source

object CDSparkContext extends Loggable {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  val mb = 1024 * 1024
  val runtime = Runtime.getRuntime

  def apply( appName: String="EDAS" ) : CDSparkContext = {
//    val cl = ClassLoader.getSystemClassLoader
//    logger.info( "Loaded jars: \n\t" + cl.asInstanceOf[java.net.URLClassLoader].getURLs.mkString("\n\t") )
//    logger.info( "EDAS env: \n\t" +  ( System.getenv.map { case (k,v) => k + ": " + v } ).mkString("\n\t") )
    val conf: SparkConf = CDSparkContext.getSparkConf( appName )
    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()
    val rv = new CDSparkContext( spark )
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel( "WARN" )
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("dag-scheduler-event-loop").setLevel(Level.WARN)
    Logger.getLogger("DAGScheduler").setLevel(Level.WARN)
    Logger.getLogger("BlockManager").setLevel(Level.WARN)
    Logger.getLogger("TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("main").setLevel(Level.WARN)

    val log_level: Level = Level.toLevel( appParameters("edas.log.level", "INFO" ) )
    Logger.getLogger("edas").setLevel(log_level)

    logger.info( "Setting SparkContext Log level to " + log_level.toString )
    logger.info( "--------------------------------------------------------")
    logger.info( "   ****  CDSparkContext Creation FINISHED  **** ")
    logger.info( "--------------------------------------------------------")
    rv
  }

  def getWorkerSignature: String = {
    val node_name = ManagementFactory.getRuntimeMXBean.getName.split("@").last.split(".").head
    val thread: Thread = Thread.currentThread()
    s"E${SparkEnv.get.executorId}:${node_name}:${thread.getName}:${thread.getId}"
  }

  def getSparkConf( appName: String  ) = {
    val edas_cache_dir = appParameters.getCacheDirectory
    val sc = new SparkConf(false)
      .setAppName( appName )
      .set("spark.local.dir", edas_cache_dir )
      .set("spark.file.transferTo", "false" )
      .set("spark.kryoserializer.buffer.max", "1000m" )
      .registerKryoClasses( Array(classOf[DirectCDTimeSliceSpec], classOf[RecordKey], classOf[CDRecord], classOf[DirectRDDVariableSpec], classOf[CDSection], classOf[HeapFltArray], classOf[Partition], classOf[CDCoordMap] ) )

    appParameters( "spark.master" ) match {
      case Some(cval) =>
        logger.info( s" >>>>> Set Spark Master from appParameters: $cval" )
        sc.setMaster(cval)
      case None => Unit
    }
    utilities.runtime.printMemoryUsage
    utilities.runtime.printMemoryUsage(logger)
    sc
  }

  def addConfig( sc: SparkConf, spark_config_id: String, edas_config_id: String ) =  appParameters( edas_config_id ) map ( cval => sc.set( spark_config_id, cval ) )

  def getPartitioner( rdd: RDD[CDRecord] ): Option[RangePartitioner] = {
    rdd.partitioner match {
      case Some( partitioner ) => partitioner match {
        case range_partitioner: RangePartitioner => Some(range_partitioner)
        case wtf => None
      }
      case None => None
    }
  }
}

class CDSparkContext(  val session: SparkSession ) extends Loggable {
  lazy val sparkContext = session.sparkContext


  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def totalClusterCores: Int = sparkContext.defaultParallelism

  def getConf: SparkConf = sparkContext.getConf

  def getPartitions( opInputs: Iterable[OperationInput] ): Option[CachePartitions] = {
    for( opInput <- opInputs ) opInput match {
      case pfrag: PartitionedFragment => return Some( pfrag.partitions )
      case _ => None
    }
    None
  }
}


