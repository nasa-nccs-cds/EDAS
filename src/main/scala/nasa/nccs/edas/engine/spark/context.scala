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

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.io.Source

object CDSparkContext extends Loggable {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  val mb = 1024 * 1024
  val totalRAM = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean].getTotalPhysicalMemorySize / mb
  val runtime = Runtime.getRuntime
  val default_executor_memory = (totalRAM-10).toString + "m"
  val default_executor_cores = (runtime.availableProcessors-1).toString
  val default_num_executors = "1"

  def apply( appName: String="EDAS", logConf: Boolean = true, enableMetrics: Boolean = false ) : CDSparkContext = {
//    val cl = ClassLoader.getSystemClassLoader
//    logger.info( "Loaded jars: \n\t" + cl.asInstanceOf[java.net.URLClassLoader].getURLs.mkString("\n\t") )
//    logger.info( "EDAS env: \n\t" +  ( System.getenv.map { case (k,v) => k + ": " + v } ).mkString("\n\t") )
    val conf: SparkConf = CDSparkContext.getSparkConf( appName, logConf, enableMetrics)
    val spark = SparkSession.builder().appName(appName).config(conf).getOrCreate()
    val rv = new CDSparkContext( spark )
    val spark_log_level = Level.toLevel( "ERROR" )
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(spark_log_level)
    spark.sparkContext.setLogLevel( "ERROR" )
    Logger.getLogger("org").setLevel(spark_log_level)
    Logger.getLogger("akka").setLevel(spark_log_level)
    Logger.getLogger("dag-scheduler-event-loop").setLevel(spark_log_level)
    Logger.getLogger("DAGScheduler").setLevel(spark_log_level)
    Logger.getLogger("BlockManager").setLevel(spark_log_level)
    Logger.getLogger("TaskSetManager").setLevel(spark_log_level)
    Logger.getLogger("main").setLevel(spark_log_level)

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

//  def merge(rdd0: RDD[CDTimeSlice], rdd1: RDD[CDTimeSlice] ): RDD[CDTimeSlice] = {
//    val t0 = System.nanoTime()
//    val mergedRdd = rdd0.join( rdd1 ) mapValues { case (part0,part1) => part0 ++ part1  } map identity
//    if ( mergedRdd.isEmpty() ) {
//      val keys0 = rdd0.keys.collect()
//      val keys1 = rdd1.keys.collect()
//      val msg = s"Empty merge ==> keys0: ${keys0.mkString(",")} keys1: ${keys1.mkString(",")}"
//      logger.error(msg)
//      throw new Exception(msg)
//    }
//    val result = rdd0.partitioner match { case Some(p) => mergedRdd.partitionBy(p); case None => rdd1.partitioner match { case Some(p) => mergedRdd.partitionBy(p); case None => mergedRdd } }
//    logger.info( "Completed MergeRDDs, time = %.4f sec".format( (System.nanoTime() - t0) / 1.0E9 ) )
//    result
//  }

//  def append(p0: CDTimeSlice, p1: CDTimeSlice ): CDTimeSlice = ( p0._1 + p1._1, p0._2.append(p1._2) )

  def getSparkConf( appName: String, logConf: Boolean, enableMetrics: Boolean  ) = {
    val edas_cache_dir = appParameters.getCacheDirectory
    val sc = new SparkConf(false)
      .setAppName( appName )
      .set("spark.logConf", logConf.toString )
      .set("spark.local.dir", edas_cache_dir )
      .set("spark.file.transferTo", "false" )
      .set("spark.kryoserializer.buffer.max", "1000m" )
      .set("spark.driver.maxResultSize", "8000m" )
      .registerKryoClasses( Array(classOf[DirectCDTimeSliceSpec], classOf[RecordKey], classOf[CDRecord], classOf[DirectRDDVariableSpec], classOf[CDSection], classOf[HeapFltArray], classOf[Partition], classOf[CDCoordMap] ) )

    val sparkConfigFile: Path = Paths.get( System.getenv("SPARK_HOME"), "conf", "spark-defaults.conf" )
    for ( raw_line <- Source.fromFile( sparkConfigFile.toFile ).getLines; line = raw_line.trim; if !(line.startsWith("#") || line.isEmpty); keyVal = line.split("\\s+") ) {
      try{
        val (key, value) = ( keyVal(0).trim, keyVal(1).trim )
        sc.set( key, value )
        logger.info( s"Add spark config value: ${key} -> ${value}")
      } catch { case ex: Exception => logger.error( "Error parsing spark parameter '" + line + ": " + ex.toString ); }
    }

    if( enableMetrics ) sc.set("spark.metrics.conf", getClass.getResource("/spark.metrics.properties").getPath )
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

//  def coalesce(rdd: RDD[CDTimeSlice], context: KernelContext ): RDD[CDTimeSlice] = {
//    if ( rdd.getNumPartitions > 1 ) {
//      getPartitioner(rdd) match {
//        case Some(partitioner) =>
//          rdd.sortByKey(true, 1) glom() map (_.fold((partitioner.range.startPoint, CDTimeSlice.empty))((x, y) => { (x._1 + y._1, x._2.append(y._2)) } ) )
//        case None => rdd
//      }
//    } else { rdd }
//  }

//  def splitPartition(key: RecordKey, part: CDTimeSlice, partitioner: RangePartitioner ): IndexedSeq[CDTimeSlice] = {
//    logger.info( s"CDSparkContext.splitPartition: KEY-${key.toString} -> PART-${part.getShape.mkString(",")}")
//    val rv = partitioner.intersect(key) map ( partkey =>
//      (partkey -> part.slice(partkey.elemStart, partkey.numElems)) )
//    rv
//  }
//
//  def applyPartitioner( partitioner: RangePartitioner )( elems: Iterator[CDTimeSlice] ): Iterator[CDTimeSlice] =
//    elems.map { case (key,part ) => splitPartition(key,part,partitioner) } reduce ( _ ++ _ ) toIterator
//
//  def repartition(rdd: RDD[CDTimeSlice], partitioner: RangePartitioner ): RDD[CDTimeSlice] =
//    rdd.mapPartitions( applyPartitioner(partitioner), true ) repartitionAndSortWithinPartitions partitioner reduceByKey(_ ++ _)

}

class CDSparkContext(  val session: SparkSession ) extends Loggable {
  implicit val strRep: LongRange.StrRep = CalendarDate.of(_).toString
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

//  def parallelize(partition: CDTimeSlice, partitioner: RangePartitioner ): RDD[CDTimeSlice] = {
//    val new_partitions = partitioner.partitions.values.map( partKey => partKey -> partition.slice( partKey.elemStart, partKey.numElems ) )
//    sparkContext parallelize new_partitions.toSeq repartitionAndSortWithinPartitions partitioner
//  }

//  def getRDD( uid: String, pFrag: PartitionedFragment, requestCx: RequestContext, opSection: Option[ma2.Section], node: WorkflowNode, batchIndex: Int, kernelContext: KernelContext ): Option[ RDD[CDTimeSlice] ] = {
//    val partitions = pFrag.partitions
//    val tgrid: TargetGrid = pFrag.getGrid
//    val rddPartSpecs: Array[RDDPartSpec] = partitions.getBatch(batchIndex) map (partition =>
//      RDDPartSpec(partition, tgrid, List(pFrag.getRDDVariableSpec(uid, partition, opSection)))
//      ) filterNot (_.empty(uid))
//    logger.info("Discarded empty partitions: Creating RDD with <<%d>> items".format( rddPartSpecs.length ))
//    if (rddPartSpecs.length == 0) { None }
//    else {
//      val partitioner = RangePartitioner( rddPartSpecs.map(_.timeRange))
//      val parallelized_rddspecs = sparkContext parallelize rddPartSpecs keyBy (_.timeRange) partitionBy partitioner
//      Some( parallelized_rddspecs mapValues (spec => spec.getRDDPartition(kernelContext,batchIndex)) )     // repartitionAndSortWithinPartitions partitioner
//    }
//  }


//  def getUnifiedRDD( directInputs: Iterable[EDASDirectDataInput], requestCx: RequestContext, batchIndex: Int ): Option[RDD[CDTimeSlice]] = {
//    val partitions = requestCx.partitioner.partitions
//    val tgrid: TargetGrid = requestCx.getTargetGrid( requestCx.partitioner.uid )
//    val batch= partitions.getBatch(batchIndex)
//    val rddPartSpecs: Array[DirectRDDPartSpec] = batch map ( partition => DirectRDDPartSpec(partition, tgrid, directInputs.map( _.getRDDVariableSpec ) ) )
//    if (rddPartSpecs.length == 0) { None }
//    else {
//      logger.info("\n **************************************************************** \n ---> Processing Batch %d: Creating input RDD with <<%d>> partitions".format(batchIndex,rddPartSpecs.length))
//      val rdd_partitioner = RangePartitioner( rddPartSpecs.map(_.timeRange) )
//      //        logger.info("Creating RDD with records:\n\t" + rddPartSpecs.flatMap( _.getCDTimeSliceSpecs() ).map( _.toString() ).mkString("\n\t"))
//      val parallelized_rddspecs = sparkContext parallelize rddPartSpecs.flatMap( _.getCDTimeSliceSpecs() ) keyBy (_.timeRange) partitionBy rdd_partitioner
//      Some( parallelized_rddspecs mapValues (spec => spec.getRDDPartition( requestCx, batchIndex )) )
//    }
//  }


//  def getRDD(uid: String, extInput: ExternalDataInput, requestCx: RequestContext, opSection: Option[ma2.Section], node: WorkflowNode, kernelContext: KernelContext, batchIndex: Int ): Option[ RDD[ CDTimeSlice ] ] = {
//    val tgrid: TargetGrid = extInput.getGrid
//    val ( key, varSpec ) = extInput.getKeyedRDDVariableSpec(uid, opSection)
//    val rddPartSpec = ExtRDDPartSpec( key, List(varSpec) )
//    val parallelized_rddspecs = sparkContext parallelize Seq( rddPartSpec ) keyBy (_.timeRange)
//    Some( parallelized_rddspecs mapValues ( spec => spec.getRDDPartition(kernelContext,batchIndex) ) )
//  }

 /* def inputConversion( dataInput: PartitionedFragment, targetGrid: TargetGrid ): PartitionedFragment = {
    dataInput.fragmentSpec.targetGridOpt match {
      case Some(inputTargetGrid) =>
        val inputTimeAxis: CoordinateAxis1DTime = inputTargetGrid.grid.getAxisSpec( 0 ).getTimeAxis
        val timeAxis: CoordinateAxis1DTime = targetGrid.grid.getAxisSpec( 0 ).getTimeAxis
        if( inputTimeAxis.equals(timeAxis) ) dataInput
        else dataInput.timeConversion( timeAxis )
      case None =>
        throw new Exception("Missing target grid for fragSpec: " + dataInput.fragmentSpec.toString)
    }
  }*/

//  def getRDD( uid: String, tVar: OperationTransientInput, tgrid: TargetGrid, partitions: Partitions, opSection: Option[ma2.Section] ): RDD[(PartitionKey,RDDPartition)] = {
//    val rddParts: IndexedSeq[(PartitionKey,RDDPartition)] = partitions.parts map { case part => ( part.getPartitionKey(tgrid) -> RDDPartition( tVar.variable.result ) ) }
////    log( " Create RDD, rddParts = " + rddParts.map(_.toXml.toString()).mkString(",") )
//    logger.info( "Creating Transient RDD with <<%d>> paritions".format( rddParts.length ) )
//    sparkContext.parallelize(rddParts)
//  }


/*
  def domainRDDPartition( opInputs: Map[String,OperationInput], context: EDASExecutionContext): RDD[(Int,RDDPartition)] = {
    val opSection: Option[ma2.Section] = context.getOpSectionIntersection
    val rdds: Iterable[RDD[(Int,RDDPartition)]] = getPartitions(opInputs.values) match {
      case Some(partitions) => opInputs.map {
          case (uid: String, pFrag: PartitionedFragment) =>
            getRDD( uid, pFrag, partitions, opSection )
          case (uid: String, tVar: OperationTransientInput ) =>
            getRDD( uid, tVar, partitions, opSection )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
      case None => opInputs.map {
          case (uid: String, tVar: OperationTransientInput ) => sparkContext.parallelize(List(0)).map(index => index -> tVar.variable.result )
          case (uid, x ) => throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
        }
    }
    if( opInputs.size == 1 ) rdds.head else rdds.tail.foldLeft( rdds.head )( CDSparkContext.merge(_,_) )
  }
  */

}


